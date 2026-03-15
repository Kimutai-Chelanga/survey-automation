// src/scripts/extraction/main.js
/**
 * Extraction pipeline with time filtering + Live Search for maximum freshness.
 *
 * FIXES (v5.4):
 *   - Removed YOUR_TWITTER_USER_ID and buildChatLink entirely.
 *     This script now only resolves and stores tweet_author_user_id.
 *     chat_link is computed by filter_links.py per-account using x_account_id
 *     from the accounts table.
 *   - Added PostgreSQL connection retry logic
 *   - Added connection heartbeat to prevent timeouts
 *   - Improved error handling for database disconnections
 */

import puppeteer from 'puppeteer-core';
import { Client } from 'pg';
import { MongoClient } from 'mongodb';
import {
  parseDatabaseUrl,
  extractTweetId,
  extractTweetTimestampFromId
} from './utils.js';
import { extractDataFromProfile } from './extraction.js';

// =============================================================================
// CONSTANTS & BUILT-IN DEFAULTS
// =============================================================================
const CHROME_DEBUG_PORT     = process.env.CHROME_DEBUG_PORT || '9222';
const SINGLE_ACCOUNT_ID     = 1;
const MAX_DB_RETRIES        = 3;
const DB_RETRY_DELAY_MS     = 5000;
const DB_HEARTBEAT_INTERVAL = 60000;

const DEFAULT_TIME_FILTER = {
  enabled:                  true,
  hours_back:               48,
  fast_mode:                true,
  skip_enhanced_extraction: false,
  max_scrolls:              15,
};

const DEFAULT_ACCOUNTS = [
  { username: 'touchofm_',      priority: 1 },
  { username: 'Eileevalencia',  priority: 2 },
  { username: 'Record_spot1',   priority: 3 },
  { username: 'brill_writers',  priority: 4 },
  { username: 'essayzpro',      priority: 5 },
  { username: 'primewriters23a',priority: 6 },
  { username: 'essaygirl01',    priority: 7 },
  { username: 'EssayNasrah',    priority: 8 },
  { username: 'Sharifwriter1',  priority: 9 },
  { username: 'EssaysAstute',   priority: 10 },
].map(a => ({
  ...a,
  display_name: a.username,
  url:          `https://x.com/${a.username}/with_replies`,
  active:       true,
}));

// =============================================================================
// STARTUP
// =============================================================================
console.log('');
console.log('█'.repeat(80));
console.log('EXTRACTION PIPELINE — LIVE SEARCH + TIME FILTERING + AUTHOR USER ID (v5.4)');
console.log('█'.repeat(80));
console.log(`Timestamp: ${new Date().toISOString()}`);
console.log('Note: chat_link NOT built here — filter_links DAG computes it via x_account_id');
console.log('█'.repeat(80));
console.log('');

const requiredEnvVars = {
  'CHROME_PROFILE_DIR': process.env.CHROME_PROFILE_DIR,
  'MONGODB_URI':        process.env.MONGODB_URI,
  'DATABASE_URL':       process.env.DATABASE_URL,
};

let hasErrors = false;
console.log('ENVIRONMENT VALIDATION');
console.log('─'.repeat(80));
for (const [varName, value] of Object.entries(requiredEnvVars)) {
  if (!value) {
    console.error(`✗ ${varName}: NOT SET`);
    hasErrors = true;
  } else {
    const display = varName.includes('URI') || varName.includes('URL')
      ? value.replace(/:[^:@]+@/, ':****@')
      : value;
    console.log(`✓ ${varName}: ${display}`);
  }
}
console.log('─'.repeat(80));
console.log('');
if (hasErrors) { console.error('❌ FATAL: Required environment variables not set'); process.exit(1); }

// =============================================================================
// POSTGRES HELPERS
// =============================================================================

async function connectPostgresWithRetry() {
  let lastError;
  for (let i = 0; i < MAX_DB_RETRIES; i++) {
    try {
      console.log(`📦 Connecting to PostgreSQL (attempt ${i + 1}/${MAX_DB_RETRIES})...`);
      const client = new Client({
        ...parseDatabaseUrl(process.env.DATABASE_URL),
        ssl: false,
        connectionTimeoutMillis: 10000,
        query_timeout: 30000,
      });
      client.on('error', (err) => console.error('⚠️ PostgreSQL client error:', err.message));
      await client.connect();
      await client.query('SELECT 1');
      console.log('✓ PostgreSQL connected and verified\n');
      return client;
    } catch (error) {
      lastError = error;
      console.warn(`⚠️ Attempt ${i + 1} failed: ${error.message}`);
      if (i < MAX_DB_RETRIES - 1) {
        console.log(`⏳ Retrying in ${DB_RETRY_DELAY_MS / 1000}s...`);
        await new Promise(r => setTimeout(r, DB_RETRY_DELAY_MS));
      }
    }
  }
  throw lastError;
}

async function ensurePostgresConnection(pgClient) {
  if (!pgClient) return null;
  try {
    await pgClient.query('SELECT 1');
    return pgClient;
  } catch {
    console.warn('⚠️ PostgreSQL connection lost, reconnecting...');
    try { await pgClient.end().catch(() => {}); } catch (_) {}
    return connectPostgresWithRetry();
  }
}

// =============================================================================
// USER ID RESOLUTION  (no chat link building)
// =============================================================================
const userIdCache = new Map();
const GQL_QUERY_ID = 'G3KGOASz96M-Qu0nwmGXNg';
const GQL_FEATURES = JSON.stringify({
  hidden_profile_likes_enabled: true,
  hidden_profile_subscriptions_enabled: true,
  rweb_tipjar_consumption_enabled: true,
  responsive_web_graphql_exclude_directive_enabled: true,
  verified_phone_label_enabled: false,
  subscriptions_verification_info_is_identity_verified_enabled: true,
  subscriptions_verification_info_verified_since_enabled: true,
  highlights_tweets_tab_ui_enabled: true,
  responsive_web_twitter_article_notes_tab_enabled: true,
  creator_subscriptions_tweet_preview_api_enabled: true,
  responsive_web_graphql_skip_user_profile_image_extensions_enabled: false,
  responsive_web_graphql_timeline_navigation_enabled: true,
});
const RESERVED = new Set(['i','home','explore','notifications','messages','search','settings','compose','intent']);

function isValidScreenName(u) {
  if (!u || typeof u !== 'string') return false;
  const n = u.replace(/^@/, '');
  return n.length >= 1 && n.length <= 50 && !RESERVED.has(n.toLowerCase()) && /^[A-Za-z0-9_]+$/.test(n);
}

async function resolveUserId(page, username) {
  if (!username) return null;
  const screenName = username.replace(/^@/, '');
  if (!isValidScreenName(screenName)) { console.warn(`  ⏭️ Invalid screen name: "${screenName}"`); return null; }
  if (userIdCache.has(screenName)) return userIdCache.get(screenName);

  try {
    console.log(`  🔍 Resolving user ID for @${screenName}...`);
    const variables = JSON.stringify({ screen_name: screenName, withSafetyModeUserFields: true });
    const apiUrl = `https://x.com/i/api/graphql/${GQL_QUERY_ID}/UserByScreenName`
      + `?variables=${encodeURIComponent(variables)}&features=${encodeURIComponent(GQL_FEATURES)}`;

    const result = await page.evaluate(async (url) => {
      try {
        const ct0  = document.cookie.split(';').map(c => c.trim()).find(c => c.startsWith('ct0='));
        const csrf = ct0 ? ct0.split('=').slice(1).join('=') : '';
        const resp = await fetch(url, {
          method: 'GET', credentials: 'include',
          headers: {
            'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs=1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
            'x-csrf-token': csrf, 'x-twitter-active-user': 'yes',
            'x-twitter-auth-type': 'OAuth2Session', 'x-twitter-client-language': 'en',
            'content-type': 'application/json',
          },
        });
        if (!resp.ok) return { userId: null, error: `HTTP ${resp.status}` };
        const data   = await resp.json();
        const result = data?.data?.user?.result;
        if (!result) return { userId: null, error: 'No user.result' };
        if (result.__typename === 'UserUnavailable') return { userId: null, error: `UserUnavailable: ${result.reason}` };
        const id = result.rest_id || result.legacy?.id_str;
        return id && /^\d+$/.test(id) ? { userId: id } : { userId: null, error: 'rest_id not found' };
      } catch (e) { return { userId: null, error: e.message }; }
    }, apiUrl);

    const { userId, error } = result || {};
    if (userId) {
      console.log(`  ✅ User ID for @${screenName}: ${userId}`);
    } else {
      console.warn(`  ⚠️ Could not resolve @${screenName}: ${error || 'unknown'}`);
    }
    userIdCache.set(screenName, userId || null);
    return userId || null;
  } catch (err) {
    console.warn(`  ⚠️ resolveUserId error for @${screenName}: ${err.message}`);
    userIdCache.set(screenName, null);
    return null;
  }
}

// =============================================================================
// MONGODB HELPERS
// =============================================================================

async function getTimeFilterSettings(mongoClient) {
  try {
    const db  = mongoClient.db('messages_db');
    const doc = await db.collection('settings').findOne({ category: 'system' });
    if (doc?.settings?.weekly_workflow_settings?.time_filter) {
      const tf = { ...DEFAULT_TIME_FILTER, ...doc.settings.weekly_workflow_settings.time_filter, enabled: true };
      if (!tf.max_scrolls || typeof tf.max_scrolls !== 'number' || tf.max_scrolls < 1)
        tf.max_scrolls = DEFAULT_TIME_FILTER.max_scrolls;
      console.log(`⏱️  Time filter (MongoDB): hours_back=${tf.hours_back} max_scrolls=${tf.max_scrolls}`);
      return tf;
    }
  } catch (e) { console.warn(`⚠️ Could not load time filter from MongoDB: ${e.message}`); }
  console.log('⏱️  Time filter (defaults): hours_back=48 max_scrolls=15');
  return { ...DEFAULT_TIME_FILTER };
}

async function getTargetAccounts(mongoClient) {
  try {
    const db   = mongoClient.db('messages_db');
    const docs = await db.collection('target_accounts').find({ active: true }).toArray();
    if (docs.length > 0) {
      console.log(`✓ ${docs.length} active accounts from MongoDB`);
      return docs.map(doc => {
        let url = doc.url || `https://x.com/${doc.username}/with_replies`;
        if (url.includes('pro.x.com') || url.includes('tweetdeck') || !url.includes('/with_replies'))
          url = `https://x.com/${doc.username}/with_replies`;
        return { username: doc.username, display_name: doc.display_name || doc.username, url, priority: doc.priority || 1, active: true };
      }).sort((a, b) => a.priority - b.priority);
    }
  } catch (e) { console.warn(`⚠️ target_accounts query failed: ${e.message}`); }
  console.log('⚠️  Using built-in default accounts');
  return DEFAULT_ACCOUNTS;
}

// =============================================================================
// INSERT LINKS  — tweet_author_user_id only, chat_link left NULL
// =============================================================================

function resolveTweetedTime(link) {
  if (link.tweetedTime) {
    try {
      const d = new Date(link.tweetedTime);
      if (!isNaN(d.getTime())) return { tweetedAt: d, tweetedDate: d.toISOString().split('T')[0] };
    } catch (_) {}
  }
  const tweetId = link.tweetId || extractTweetId(link.url);
  if (tweetId) {
    const sf = extractTweetTimestampFromId(tweetId);
    if (sf?.timestamp) {
      const d = new Date(sf.timestamp * 1000);
      return { tweetedAt: d, tweetedDate: d.toISOString().split('T')[0] };
    }
  }
  return { tweetedAt: null, tweetedDate: null };
}

async function insertLinks(pgClient, links, isParent = false, cutoffTime = null, page = null) {
  let inserted = 0, duplicates = 0, errors = 0, userIdResolved = 0;

  for (const link of links) {
    try {
      const tweetId                    = link.tweetId || extractTweetId(link.url);
      const { tweetedAt, tweetedDate } = resolveTweetedTime(link);

      if (cutoffTime && tweetedAt && tweetedAt < cutoffTime) continue;

      let tweetAuthorUserId = null;
      if (link.author && page) {
        tweetAuthorUserId = await resolveUserId(page, link.author);
        if (tweetAuthorUserId) userIdResolved++;
      }

      // chat_link column intentionally omitted — filter_links.py sets it per-account
      const result = await pgClient.query(`
        INSERT INTO links (
          link, tweet_id, account_id, is_parent_tweet,
          tweeted_time, tweeted_date, scraped_time,
          workflow_status, within_limit, used, filtered,
          tweet_author_user_id
        )
        VALUES ($1,$2,$3,$4,$5,$6,CURRENT_TIMESTAMP,'pending',FALSE,FALSE,FALSE,$7)
        ON CONFLICT (link) DO UPDATE
          SET tweet_author_user_id = COALESCE(EXCLUDED.tweet_author_user_id, links.tweet_author_user_id)
        RETURNING links_id, (xmax = 0) AS inserted_new
      `, [link.url, tweetId, SINGLE_ACCOUNT_ID, isParent, tweetedAt, tweetedDate, tweetAuthorUserId]);

      result.rows[0]?.inserted_new ? inserted++ : duplicates++;
    } catch (error) {
      errors++;
      console.error(`  ✗ Error inserting ${link.url}: ${error.message}`);
    }
  }
  return { inserted, duplicates, errors, userIdResolved };
}

// =============================================================================
// SESSION PAGE
// =============================================================================

async function openSessionPage(browser) {
  const { loadSession } = await import('./persistent_session.js');
  const page = await browser.newPage();
  page.on('console', msg => {
    const t = msg.type();
    const s = msg.text();
    if (t === 'error') console.error(`  [PAGE] ${s}`);
    else if (t === 'warning') console.warn(`  [PAGE] ${s}`);
    else console.log(`  [PAGE] ${s}`);
  });
  page.on('pageerror', err => console.error(`  [PAGE ERROR] ${err.message}`));
  await loadSession(page);

  try {
    await page.waitForFunction(
      () => window.location.href.includes('x.com') || window.location.href.includes('twitter.com'),
      { timeout: 15000 }
    );
    await new Promise(r => setTimeout(r, 2000));
    await page.waitForSelector('article', { timeout: 10000 }).catch(() =>
      console.log('  ⚠️ No articles found — check login')
    );
    console.log('  ✓ Session page ready on x.com');
  } catch (e) { console.warn(`  ⚠️ Page load warning: ${e.message}`); }

  return page;
}

// =============================================================================
// MAIN
// =============================================================================
(async () => {
  let pgClient = null, mongoClient = null, browser = null, sessionPage = null;
  let lastHeartbeat = Date.now();

  try {
    const batchId = `batch_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    mongoClient = new MongoClient(process.env.MONGODB_URI,
      { serverSelectionTimeoutMS: 5000, connectTimeoutMS: 10000, socketTimeoutMS: 45000 });
    await mongoClient.connect();
    console.log('✓ MongoDB connected\n');

    const timeFilter     = await getTimeFilterSettings(mongoClient);
    const targetAccounts = await getTargetAccounts(mongoClient);
    if (!targetAccounts.length) throw new Error('No target accounts available');

    browser = await puppeteer.connect({ browserURL: `http://localhost:${CHROME_DEBUG_PORT}`, defaultViewport: null });
    console.log('✓ Chrome connected\n');

    pgClient    = await connectPostgresWithRetry();
    sessionPage = await openSessionPage(browser);

    // Cookie check
    try {
      const url = await sessionPage.evaluate(() => window.location.href);
      if (!url.includes('x.com') && !url.includes('twitter.com')) {
        await sessionPage.goto('https://x.com/home', { waitUntil: 'domcontentloaded' });
        await new Promise(r => setTimeout(r, 3000));
      }
      const ck = await sessionPage.evaluate(() => {
        const names = document.cookie.split(';').filter(c => c.trim()).map(c => c.trim().split('=')[0]);
        return { hasCt0: names.includes('ct0'), hasAuthToken: names.includes('auth_token'), count: names.length };
      });
      console.log(`🍪 Cookies: ${ck.count} total | ct0=${ck.hasCt0} | auth_token=${ck.hasAuthToken}`);
      if (!ck.hasCt0 || !ck.hasAuthToken)
        console.warn('⚠️ Missing auth cookies — GraphQL user ID lookups may fail');
    } catch (e) { console.warn(`⚠️ Cookie check failed: ${e.message}`); }
    console.log('');

    console.log('━'.repeat(80));
    console.log('EXTRACTION MODE');
    console.log(`  Source:          ✅ Live Search`);
    console.log(`  Time filter:     ✅ LAST ${timeFilter.hours_back} HOURS`);
    console.log(`  Max scrolls:     ${timeFilter.max_scrolls}`);
    console.log(`  User ID extract: ✅ tweet_author_user_id column`);
    console.log(`  Chat link:       ⏭️  SKIPPED — filter_links.py handles per-account via x_account_id`);
    console.log('━'.repeat(80));
    console.log('');

    const stats = {
      accountsProcessed: 0, totalInserted: 0, totalDuplicates: 0,
      totalErrors: 0, totalUserIdsResolved: 0,
    };
    const cutoffTime = new Date(Date.now() - timeFilter.hours_back * 60 * 60 * 1000);

    for (const account of targetAccounts) {
      if (!account.active) { console.log(`⏭️  Skipping inactive: @${account.username}`); continue; }

      if (Date.now() - lastHeartbeat > DB_HEARTBEAT_INTERVAL) {
        pgClient      = await ensurePostgresConnection(pgClient);
        lastHeartbeat = Date.now();
      }

      console.log('█'.repeat(80));
      console.log(`EXTRACTING: @${account.username}`);
      console.log('█'.repeat(80));

      const { links, parentLinks } = await extractDataFromProfile(
        browser, account.url, pgClient,
        !timeFilter.skip_enhanced_extraction,
        timeFilter.hours_back, timeFilter.fast_mode,
        timeFilter.max_scrolls, account.username
      );

      console.log(`✓ Profile links: ${links.length}  Parent links: ${parentLinks.length}\n`);

      const pr = await insertLinks(pgClient, links, false, cutoffTime, sessionPage);
      console.log(`   inserted=${pr.inserted}  dupes=${pr.duplicates}  errors=${pr.errors}  userIds=${pr.userIdResolved}\n`);

      let ar = { inserted: 0, duplicates: 0, errors: 0, userIdResolved: 0 };
      if (parentLinks.length > 0) {
        ar = await insertLinks(pgClient, parentLinks, true, cutoffTime, sessionPage);
        console.log(`   (parents) inserted=${ar.inserted}  dupes=${ar.duplicates}  errors=${ar.errors}  userIds=${ar.userIdResolved}\n`);
      }

      stats.totalInserted        += pr.inserted + ar.inserted;
      stats.totalDuplicates      += pr.duplicates + ar.duplicates;
      stats.totalErrors          += pr.errors + ar.errors;
      stats.totalUserIdsResolved += pr.userIdResolved + ar.userIdResolved;
      stats.accountsProcessed++;
    }

    console.log('═'.repeat(80));
    console.log('SUMMARY');
    console.log(`  Batch ID:            ${batchId}`);
    console.log(`  Accounts processed:  ${stats.accountsProcessed}/${targetAccounts.length}`);
    console.log(`  Total inserted:      ${stats.totalInserted}`);
    console.log(`  Total duplicates:    ${stats.totalDuplicates}`);
    console.log(`  Total errors:        ${stats.totalErrors}`);
    console.log(`  User IDs resolved:   ${stats.totalUserIdsResolved}`);
    console.log(`  Chat links:          ⏭️  Computed by filter_links.py via x_account_id`);
    console.log(`  Completed:           ${new Date().toISOString()}`);
    console.log('═'.repeat(80));

  } catch (error) {
    console.error(`\n❌ PIPELINE FAILED: ${error.message}`);
    console.error(error.stack);
    process.exitCode = 1;
  } finally {
    if (sessionPage)  await sessionPage.close().catch(() => {});
    if (browser)      await browser.disconnect().catch(() => {});
    if (pgClient)     await pgClient.end().catch(() => {});
    if (mongoClient)  await mongoClient.close().catch(() => {});
    console.log('✓ Done\n');
  }
})();
