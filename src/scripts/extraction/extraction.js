// src/scripts/extraction/extraction.js
/**
 * Extraction with MAXIMUM FRESHNESS — Live Search + Always-On Parent Extraction
 *
 * FIXES (v5.1):
 *   - Author extraction now uses data-testid="User-Name" which is the stable
 *     X DOM testid for the tweet author's name block. Falls back to the first
 *     /status/ URL segment (tweet URL already contains the username).
 *     The old selector (a[href^="/"][role="link"]:not([href*="/status/"])) was
 *     returning wrong authors (nav links, "Replying to" links, etc.)
 *
 * IMPROVEMENTS (v4):
 *
 * 1. SCROLL UNTIL CUTOFF (not fixed scroll count)
 *    - Scrolls until the oldest visible tweet is older than the cutoff timestamp
 *    - Max scroll count is now configurable via MongoDB (default: 15)
 *    - Makes extraction deterministic relative to the 48h window
 *
 * 2. LAST SEEN TWEET ID TRACKING
 *    - After each account run the newest tweet ID seen is persisted to PostgreSQL
 *      in the account_extraction_state table
 *    - On the next run, scrolling also stops once that tweet ID re-appears
 *    - Makes extraction incremental — never re-processes already-seen tweets
 *
 * 3. EMPTY LOAD / RATE LIMIT DETECTION
 *    - If a scroll yields no new <article> elements, wait 3 s and retry once
 *    - If the retry also yields nothing, log a throttling warning and stop
 *    - Prevents silently accepting partial loads
 *
 * ENHANCED PARENT EXTRACTION IS ALWAYS ON:
 *   Every reply tweet found is opened individually to fetch the parent tweet.
 */

import { loadSession, saveSession } from './persistent_session.js';
import { extractTweetId, extractTweetTimestampFromId } from './utils.js';

const STALE_SCROLL_LIMIT        = 3;
const DEFAULT_MAX_SCROLLS       = 15;   // used when MongoDB setting is absent
const EMPTY_LOAD_RETRY_DELAY_MS = 3000; // wait before retry on empty scroll
const BETWEEN_SCROLL_DELAY_MS   = 1500; // normal inter-scroll delay

// =============================================================================
// URL BUILDER
// =============================================================================

function buildLiveSearchUrl(accountUrl) {
  const match = accountUrl.match(/x\.com\/([^/?#]+)/);
  if (!match) {
    console.warn(`⚠️ Could not parse username from URL: ${accountUrl} — using as-is`);
    return accountUrl;
  }

  const username = match[1];
  const reserved = ['search', 'home', 'explore', 'notifications', 'messages', 'i'];
  if (reserved.includes(username.toLowerCase())) {
    console.warn(`⚠️ Non-profile URL: ${accountUrl} — using as-is`);
    return accountUrl;
  }

  const liveUrl = `https://x.com/search?q=from%3A${username}&src=typed_query&f=live`;
  console.log(`  🔀 Profile → Live Search`);
  console.log(`     From: ${accountUrl}`);
  console.log(`     To:   ${liveUrl}`);
  return liveUrl;
}

// =============================================================================
// DB CUTOFF & TIME FILTER
// =============================================================================

async function getCutoffTime(pgClient, hoursBack = 0) {
  if (hoursBack > 0) {
    const cutoff = new Date(Date.now() - hoursBack * 60 * 60 * 1000);
    console.log(`⏱️  Time filter: ${hoursBack}h back → cutoff ${cutoff.toISOString().substring(0, 16)}`);
    return cutoff;
  }

  if (!pgClient) {
    console.log('📊 No DB client — scrolling full depth');
    return null;
  }

  try {
    const result = await pgClient.query(`
      SELECT MAX(tweeted_time) AS newest
      FROM links
      WHERE tweeted_time IS NOT NULL AND account_id = $1
    `, [1]);

    const newest = result.rows[0]?.newest;
    if (newest) {
      const cutoff = new Date(newest);
      console.log(`📊 DB cutoff: ${cutoff.toISOString().substring(0, 16)}`);
      return cutoff;
    }

    console.log('📊 No existing tweets in DB — scrolling full depth');
    return null;
  } catch (err) {
    console.warn(`⚠️  DB cutoff query failed: ${err.message}`);
    return null;
  }
}

// =============================================================================
// LAST SEEN TWEET ID — READ & WRITE
// =============================================================================

/**
 * Read the last-seen tweet ID for a given account username from PostgreSQL.
 * Returns null if the row does not exist or the table is missing.
 */
async function getLastSeenTweetId(pgClient, username) {
  if (!pgClient) return null;
  try {
    const result = await pgClient.query(`
      SELECT last_seen_tweet_id
      FROM account_extraction_state
      WHERE username = $1
    `, [username]);
    const id = result.rows[0]?.last_seen_tweet_id || null;
    if (id) {
      console.log(`🔖 Last seen tweet ID for @${username}: ${id}`);
    } else {
      console.log(`🔖 No last seen tweet ID for @${username} — full scan`);
    }
    return id;
  } catch (err) {
    // Table may not exist yet — that's fine
    console.warn(`⚠️  Could not read last_seen_tweet_id for @${username}: ${err.message}`);
    return null;
  }
}

/**
 * Persist the newest tweet ID seen in this run so the next run can stop early.
 */
async function saveLastSeenTweetId(pgClient, username, tweetId) {
  if (!pgClient || !tweetId) return;
  try {
    await pgClient.query(`
      INSERT INTO account_extraction_state (username, last_seen_tweet_id, last_extraction_time)
      VALUES ($1, $2, NOW())
      ON CONFLICT (username) DO UPDATE
        SET last_seen_tweet_id  = EXCLUDED.last_seen_tweet_id,
            last_extraction_time = EXCLUDED.last_extraction_time
    `, [username, tweetId]);
    console.log(`💾 Saved last_seen_tweet_id=${tweetId} for @${username}`);
  } catch (err) {
    console.warn(`⚠️  Could not save last_seen_tweet_id: ${err.message}`);
  }
}

// =============================================================================
// FRESH PAGE LOAD
// =============================================================================

async function navigateAndForceRefresh(page, targetUrl) {
  const liveUrl = buildLiveSearchUrl(targetUrl);

  console.log('');
  console.log('━'.repeat(80));
  console.log('FRESH PAGE LOAD — LIVE SEARCH + CACHE-BUST HEADERS');
  console.log('━'.repeat(80));
  console.log(`  URL: ${liveUrl}`);

  await page.setExtraHTTPHeaders({
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma':        'no-cache',
    'Expires':       '0',
  });
  console.log('  ✓ Cache-busting headers set');

  const navStart = Date.now();
  console.log('  → Navigating...');
  await page.goto(liveUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
  console.log(`  ✓ DOM ready (${Date.now() - navStart}ms)`);

  await page.evaluate(() => window.scrollTo(0, 0));

  try {
    await page.waitForSelector('article', { timeout: 15000 });
    console.log('  ✓ Articles detected');
  } catch {
    console.warn('  ⚠️ No articles found — search may have returned 0 results');
  }

  try {
    await page.waitForFunction(
      () => document.querySelectorAll('article').length >= 3,
      { timeout: 10000 }
    );
    const count = await page.evaluate(() => document.querySelectorAll('article').length);
    console.log(`  ✓ ${count} articles visible`);
  } catch {
    console.warn('  ⚠️ Fewer than 3 articles loaded');
  }

  await new Promise(r => setTimeout(r, 1500));

  const topTs = await page.evaluate(() => {
    const times = [];
    document.querySelectorAll('article time[datetime]').forEach(el => {
      const v = el.getAttribute('datetime');
      if (v) times.push(v);
    });
    return times;
  });

  if (topTs.length > 0) {
    const newest     = new Date(Math.max(...topTs.map(d => new Date(d).getTime())));
    const oldest     = new Date(Math.min(...topTs.map(d => new Date(d).getTime())));
    const ageMinutes = Math.round((Date.now() - newest.getTime()) / 60000);
    console.log(`  📌 Newest tweet: ${newest.toISOString().substring(0, 16)} (${ageMinutes} min ago)`);
    console.log(`  📌 Oldest tweet: ${oldest.toISOString().substring(0, 16)}`);
    console.log(`  📌 Visible:      ${topTs.length} timestamps`);
  } else {
    console.warn('  ⚠️ No timestamps found after load');
  }

  console.log(`  ⏱️  Total nav time: ${Date.now() - navStart}ms`);
  console.log('━'.repeat(80));
  console.log('');
}

// =============================================================================
// ARTICLE SNAPSHOT HELPER
// Returns { dates: string[], tweetIds: string[], articleCount: number }
// =============================================================================

async function getPageSnapshot(page) {
  return page.evaluate(() => {
    const dates    = [];
    const tweetIds = [];
    document.querySelectorAll('article').forEach(article => {
      const timeEl = article.querySelector('time[datetime]');
      if (timeEl) dates.push(timeEl.getAttribute('datetime'));

      const linkEl = article.querySelector('a[href*="/status/"]');
      if (linkEl) {
        const m = linkEl.href.match(/\/status\/(\d+)/);
        if (m) tweetIds.push(m[1]);
      }
    });
    return { dates, tweetIds, articleCount: document.querySelectorAll('article').length };
  });
}

// =============================================================================
// UNIFIED SCROLL — SCROLL UNTIL CUTOFF + LAST SEEN ID + EMPTY LOAD DETECTION
// =============================================================================

/**
 * Scroll until:
 *   a) The oldest visible tweet is older than cutoffTime, OR
 *   b) The last-seen tweet ID re-appears on the page (incremental stop), OR
 *   c) We hit the page end (no new height), OR
 *   d) We reach maxScrolls
 *
 * On each scroll, if no new <article> elements appear, wait EMPTY_LOAD_RETRY_DELAY_MS
 * and retry once before giving up (rate-limit / empty-load guard).
 */
async function scrollUntilCutoff(
  page,
  cutoffTime,
  lastSeenTweetId,
  maxScrolls = DEFAULT_MAX_SCROLLS,
  scrollDelay = BETWEEN_SCROLL_DELAY_MS
) {
  const cutoffStr = cutoffTime
    ? cutoffTime.toISOString().substring(0, 16)
    : 'none';

  console.log('');
  console.log('━'.repeat(80));
  console.log(`SCROLL STRATEGY: scroll-until-cutoff`);
  console.log(`  Cutoff:           ${cutoffStr}`);
  console.log(`  Last seen ID:     ${lastSeenTweetId || 'none'}`);
  console.log(`  Max scrolls:      ${maxScrolls}`);
  console.log('━'.repeat(80));

  // Pre-scroll snapshot
  const preSnap = await getPageSnapshot(page);
  if (preSnap.dates.length > 0) {
    const newest = new Date(Math.max(...preSnap.dates.map(d => new Date(d).getTime())));
    const age    = Math.round((Date.now() - newest.getTime()) / 60000);
    console.log(`  📌 Pre-scroll newest: ${newest.toISOString().substring(0, 16)} (${age} min ago)`);
  }

  let previousHeight  = 0;
  let scrollCount     = 0;
  let pageEndCount    = 0;
  let previousArticleCount = preSnap.articleCount;
  let newestTweetIdSeen    = null;

  // Track the globally newest tweet ID across all scrolls (for saving at the end)
  if (preSnap.tweetIds.length > 0) {
    newestTweetIdSeen = preSnap.tweetIds.reduce((max, id) =>
      BigInt(id) > BigInt(max) ? id : max
    );
  }

  for (let i = 0; i < maxScrolls; i++) {
    // ── SCROLL ──────────────────────────────────────────────────────────────
    const currentHeight = await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
      return document.body.scrollHeight;
    });

    await new Promise(r => setTimeout(r, scrollDelay));

    // ── PAGE-END DETECTION ───────────────────────────────────────────────────
    if (currentHeight === previousHeight) {
      pageEndCount++;
      if (pageEndCount >= 2) {
        console.log(`  Scroll ${i + 1}: ✅ Page end reached (height unchanged ${pageEndCount}x)`);
        break;
      }
    } else {
      pageEndCount = 0;
    }
    previousHeight = currentHeight;

    // ── SNAPSHOT ─────────────────────────────────────────────────────────────
    let snap = await getPageSnapshot(page);

    // ── EMPTY LOAD DETECTION ─────────────────────────────────────────────────
    if (snap.articleCount === previousArticleCount) {
      console.log(`  Scroll ${i + 1}: ⚠️  No new articles (${snap.articleCount} same as before) — waiting ${EMPTY_LOAD_RETRY_DELAY_MS}ms and retrying`);
      await new Promise(r => setTimeout(r, EMPTY_LOAD_RETRY_DELAY_MS));

      // Retry snapshot
      snap = await getPageSnapshot(page);

      if (snap.articleCount === previousArticleCount) {
        console.log(`  Scroll ${i + 1}: 🚫 Still no new articles after retry — possible rate limit / end of feed`);
        console.log(`             Stopping scroll to avoid silent partial load`);
        break;
      } else {
        console.log(`  Scroll ${i + 1}: ✅ Articles appeared after retry (${snap.articleCount})`);
      }
    }
    previousArticleCount = snap.articleCount;

    // ── LAST SEEN ID CHECK ───────────────────────────────────────────────────
    if (lastSeenTweetId && snap.tweetIds.includes(lastSeenTweetId)) {
      console.log(`  Scroll ${i + 1}: 🔖 Last seen tweet ID ${lastSeenTweetId} re-appeared — incremental stop`);
      scrollCount++;
      break;
    }

    // ── UPDATE NEWEST TWEET ID ────────────────────────────────────────────────
    if (snap.tweetIds.length > 0) {
      const localNewest = snap.tweetIds.reduce((max, id) =>
        BigInt(id) > BigInt(max) ? id : max
      );
      if (!newestTweetIdSeen || BigInt(localNewest) > BigInt(newestTweetIdSeen)) {
        newestTweetIdSeen = localNewest;
      }
    }

    // ── CUTOFF CHECK ─────────────────────────────────────────────────────────
    if (snap.dates.length > 0 && cutoffTime) {
      const validDates = snap.dates.map(d => new Date(d)).filter(d => !isNaN(d.getTime()));
      if (validDates.length > 0) {
        const oldest  = new Date(Math.min(...validDates.map(d => d.getTime())));
        const newest  = new Date(Math.max(...validDates.map(d => d.getTime())));
        const ageMin  = Math.round((Date.now() - newest.getTime()) / 60000);

        console.log(
          `  Scroll ${i + 1}: height=${currentHeight}px | ` +
          `articles=${snap.articleCount} | ` +
          `newest=${newest.toISOString().substring(0, 16)} (${ageMin}m ago) | ` +
          `oldest=${oldest.toISOString().substring(0, 16)}`
        );

        if (oldest < cutoffTime) {
          console.log(`    ✅ Oldest tweet (${oldest.toISOString().substring(0, 16)}) < cutoff (${cutoffStr}) — stopping`);
          scrollCount++;
          break;
        }
      }
    } else {
      console.log(`  Scroll ${i + 1}: height=${currentHeight}px | articles=${snap.articleCount} | no timestamps`);
    }

    scrollCount++;
  }

  console.log(`\n✅ Scroll complete — ${scrollCount} scrolls, newest tweet ID: ${newestTweetIdSeen || 'unknown'}`);
  console.log('━'.repeat(80));
  console.log('');

  return { newestTweetIdSeen };
}

// =============================================================================
// EXTRACT TWEETS FROM PAGE DOM
// =============================================================================

/**
 * Extract the tweet author's screen name from an article element.
 *
 * Strategy (in order of reliability):
 *   1. data-testid="User-Name" block — the dedicated author name container
 *      X uses on every tweet card. The first <a href="/{username}"> inside it
 *      is always the author.
 *   2. Derive from the tweet URL itself — the tweet URL is always
 *      https://x.com/{username}/status/{id}, so we can parse the username
 *      directly without touching the DOM at all.
 *
 * The old approach (a[href^="/"][role="link"]:not([href*="/status/"])) was
 * unreliable: it matched nav links, "Replying to @X" anchors, and other
 * role="link" elements that appear before the author block.
 *
 * @param {Element} article  - a tweet <article> element
 * @param {string}  tweetUrl - the canonical tweet URL (https://x.com/user/status/id)
 * @returns {string}  screen name without @, or '' if not found
 */
function extractAuthorFromArticle(article, tweetUrl) {
  // Strategy 1: data-testid="User-Name" container
  const userNameBlock = article.querySelector('[data-testid="User-Name"]');
  if (userNameBlock) {
    // The first <a> inside this block links to the author's profile: href="/username"
    const profileLink = userNameBlock.querySelector('a[href^="/"]');
    if (profileLink) {
      const href = profileLink.getAttribute('href');
      // href is "/username" — strip the leading slash
      // Skip reserved paths that are not usernames
      const candidate = href.replace(/^\//, '').split('/')[0];
      const reserved  = ['i', 'home', 'explore', 'notifications', 'messages', 'search', 'settings'];
      if (candidate && !reserved.includes(candidate.toLowerCase())) {
        return candidate;
      }
    }
  }

  // Strategy 2: parse from the tweet URL (always https://x.com/{username}/status/{id})
  if (tweetUrl) {
    const m = tweetUrl.match(/x\.com\/([^/]+)\/status\//);
    if (m && m[1]) return m[1];
  }

  return '';
}

async function extractLinksAndParents(page, cutoffTime = null) {
  console.log('\n🔍 Extracting tweets from DOM...');

  const result = await page.evaluate((cutoffTimeStr) => {
    const cutoff = cutoffTimeStr ? new Date(cutoffTimeStr) : null;

    /**
     * Extract author from article using data-testid="User-Name" (primary)
     * or from the tweet URL (fallback).
     * Defined inline because page.evaluate runs in browser context.
     */
    function extractAuthor(article, tweetUrl) {
      // Primary: dedicated author name block
      const userNameBlock = article.querySelector('[data-testid="User-Name"]');
      if (userNameBlock) {
        const profileLink = userNameBlock.querySelector('a[href^="/"]');
        if (profileLink) {
          const candidate = profileLink.getAttribute('href').replace(/^\//, '').split('/')[0];
          const reserved  = ['i', 'home', 'explore', 'notifications', 'messages', 'search', 'settings'];
          if (candidate && !reserved.includes(candidate.toLowerCase())) {
            return candidate;
          }
        }
      }

      // Fallback: parse from tweet URL
      if (tweetUrl) {
        const m = tweetUrl.match(/x\.com\/([^/]+)\/status\//);
        if (m && m[1]) return m[1];
      }

      return '';
    }

    function getArticleData(article) {
      const timeLink =
        article.querySelector('a:has(time[datetime])') ||
        article.querySelector('a[href*="/status/"]');
      if (!timeLink) return null;

      const cleanUrl = timeLink.href.split('?')[0].split('#')[0];
      const match    = cleanUrl.match(/\/status\/(\d+)/);
      if (!match) return null;

      const timeEl    = article.querySelector('time[datetime]');
      const textEl    = article.querySelector('[data-testid="tweetText"]');

      // ── FIXED: use reliable author extraction ─────────────────────────────
      const author = extractAuthor(article, cleanUrl);

      const isQuoted  = !!article.querySelector('[data-testid="quotedTweet"]');
      const isReply   =
        article.textContent.includes('Replying to') ||
        !!article.querySelector('[data-testid="socialContext"]');
      const tweetTime = timeEl?.getAttribute('datetime');

      return {
        url:         cleanUrl,
        tweetId:     match[1],
        tweetedTime: tweetTime,
        text:        (textEl?.innerText.trim().slice(0, 500) || ''),
        author,
        isQuoted,
        isReply,
        source:  isQuoted ? 'quoted' : (isReply ? 'reply' : 'original'),
        _isOld:  cutoff && tweetTime ? new Date(tweetTime) < cutoff : false,
      };
    }

    const profileLinks = [];
    const seenUrls     = new Set();
    const cells        = [...document.querySelectorAll('div[data-testid="cellInnerDiv"]')];

    for (const cell of cells) {
      const articles = [...cell.querySelectorAll('article')];
      if (!articles.length) continue;

      for (let i = 0; i < articles.length; i++) {
        const data = getArticleData(articles[i]);
        if (!data || seenUrls.has(data.url) || data._isOld) continue;
        seenUrls.add(data.url);
        profileLinks.push(data);
      }
    }

    return { profileLinks };
  }, cutoffTime ? cutoffTime.toISOString() : null);

  const { profileLinks = [] } = result;

  // Deduplicate
  const seen   = new Set();
  const unique = [];
  for (const link of profileLinks) {
    if (!seen.has(link.url)) { seen.add(link.url); unique.push(link); }
  }

  // Freshness summary
  const timestamps = unique.filter(l => l.tweetedTime).map(l => new Date(l.tweetedTime));
  if (timestamps.length > 0) {
    const freshest = new Date(Math.max(...timestamps));
    const oldest   = new Date(Math.min(...timestamps));
    const ageMin   = Math.round((Date.now() - freshest.getTime()) / 60000);
    console.log(`\n  🕐 Freshest extracted: ${freshest.toISOString().substring(0, 16)} (${ageMin} min ago)`);
    console.log(`  🕐 Oldest extracted:   ${oldest.toISOString().substring(0, 16)}`);
  }

  // Log author resolution stats
  const withAuthor    = unique.filter(l => l.author).length;
  const withoutAuthor = unique.length - withAuthor;
  if (withoutAuthor > 0) {
    console.warn(`  ⚠️  ${withoutAuthor} tweets have no author resolved (will skip user ID lookup)`);
  }

  const originals = unique.filter(l => !l.isReply && !l.isQuoted);
  const replies   = unique.filter(l => l.isReply);
  const quoted    = unique.filter(l => l.isQuoted);

  console.log(`\n✅ Extracted: ${unique.length} tweets`);
  console.log(`   - ${originals.length} originals | ${replies.length} replies | ${quoted.length} quoted`);
  console.log(`   - ${withAuthor} with author resolved | ${withoutAuthor} without`);

  return unique;
}

// =============================================================================
// PARENT EXTRACTION — ALWAYS ON, NO CAP
// =============================================================================

async function extractAllParentTweets(browser, profileLinks, cutoffTime = null) {
  const replies = profileLinks.filter(l => l.isReply);

  console.log('');
  console.log('━'.repeat(80));
  console.log(`PARENT EXTRACTION — ALWAYS ON — ${replies.length} replies to process`);
  console.log('━'.repeat(80));

  if (replies.length === 0) {
    console.log('  No replies found — nothing to extract parents for');
    console.log('━'.repeat(80));
    return [];
  }

  const page         = await browser.newPage();
  const parentLinks  = [];
  const seenUrls     = new Set();
  let   successCount = 0;
  let   failCount    = 0;
  let   skippedOld   = 0;

  await loadSession(page);

  for (let i = 0; i < replies.length; i++) {
    const reply = replies[i];
    console.log(`\n  [${i + 1}/${replies.length}] Reply: ${reply.url}`);
    console.log(`    Author: @${reply.author}  TweetID: ${reply.tweetId}`);

    try {
      await page.goto(reply.url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await page.waitForSelector('article', { timeout: 10000 });
      await new Promise(r => setTimeout(r, 1000));

      const parentData = await page.evaluate((replyUrl) => {
        /**
         * Inline author extractor — same logic as in extractLinksAndParents.
         * Must be defined inside page.evaluate (browser context).
         */
        function extractAuthor(article, tweetUrl) {
          const userNameBlock = article.querySelector('[data-testid="User-Name"]');
          if (userNameBlock) {
            const profileLink = userNameBlock.querySelector('a[href^="/"]');
            if (profileLink) {
              const candidate = profileLink.getAttribute('href').replace(/^\//, '').split('/')[0];
              const reserved  = ['i', 'home', 'explore', 'notifications', 'messages', 'search', 'settings'];
              if (candidate && !reserved.includes(candidate.toLowerCase())) {
                return candidate;
              }
            }
          }
          if (tweetUrl) {
            const m = tweetUrl.match(/x\.com\/([^/]+)\/status\//);
            if (m && m[1]) return m[1];
          }
          return '';
        }

        const articles = Array.from(document.querySelectorAll('article'));

        for (const article of articles) {
          const isReply =
            article.textContent?.includes('Replying to') ||
            !!article.querySelector('[data-testid="socialContext"]');
          if (isReply) continue;

          const timeLink =
            article.querySelector('a:has(time[datetime])') ||
            article.querySelector('a[href*="/status/"]');
          if (!timeLink) continue;

          const cleanUrl = timeLink.href.split('?')[0].split('#')[0];
          const match    = cleanUrl.match(/\/status\/(\d+)/);
          if (!match) continue;

          if (cleanUrl === replyUrl) continue;

          const timeEl       = article.querySelector('time[datetime]');
          const textEl       = article.querySelector('[data-testid="tweetText"]');
          const parentAuthor = extractAuthor(article, cleanUrl);

          return {
            url:         cleanUrl,
            tweetId:     match[1],
            tweetedTime: timeEl?.getAttribute('datetime') || null,
            text:        textEl ? textEl.innerText.trim().slice(0, 500) : '',
            author:      parentAuthor,
            isParent:    true,
            replyUrl:    replyUrl,
            source:      'parent',
          };
        }

        return null;
      }, reply.url);

      if (!parentData) {
        console.log(`    ⚠️  No parent found (may be deleted or protected)`);
        failCount++;
        continue;
      }

      if (cutoffTime && parentData.tweetedTime) {
        const parentDate = new Date(parentData.tweetedTime);
        if (parentDate < cutoffTime) {
          console.log(`    ⏭️  Parent older than cutoff — skipping`);
          skippedOld++;
          continue;
        }
      }

      if (seenUrls.has(parentData.url)) {
        console.log(`    ↩️  Duplicate parent — skipping`);
        continue;
      }

      seenUrls.add(parentData.url);
      parentLinks.push(parentData);
      successCount++;

      const ageMin = parentData.tweetedTime
        ? Math.round((Date.now() - new Date(parentData.tweetedTime).getTime()) / 60000)
        : null;

      console.log(`    ✅ Parent found: @${parentData.author}`);
      console.log(`       URL:  ${parentData.url}`);
      console.log(`       Age:  ${ageMin !== null ? `${ageMin} min ago` : 'unknown'}`);
      console.log(`       Text: ${parentData.text.substring(0, 80)}${parentData.text.length > 80 ? '...' : ''}`);

      await new Promise(r => setTimeout(r, 800));

    } catch (err) {
      console.error(`    ❌ Failed: ${err.message}`);
      failCount++;
    }
  }

  await page.close();

  console.log('');
  console.log('━'.repeat(80));
  console.log(`PARENT EXTRACTION COMPLETE`);
  console.log(`  Processed:   ${replies.length} replies`);
  console.log(`  ✅ Found:    ${successCount} parents`);
  console.log(`  ❌ Failed:   ${failCount} (deleted/protected/error)`);
  console.log(`  ⏭️  Skipped: ${skippedOld} (older than cutoff)`);
  console.log('━'.repeat(80));
  console.log('');

  return parentLinks;
}

// =============================================================================
// MAIN EXPORT
// =============================================================================

/**
 * Extract tweets from an account using Live Search, then extract ALL parent
 * tweets for every reply found.
 *
 * @param {object}  browser
 * @param {string}  targetUrl
 * @param {object}  pgClient        - PostgreSQL client (for cutoff + state tracking)
 * @param {boolean} extractParents  - always true; kept for API compat
 * @param {number}  hoursBack       - time filter window
 * @param {boolean} fastMode        - unused; scroll strategy is now always cutoff-based
 * @param {number}  maxScrolls      - configurable max scroll count (default 15)
 * @param {string}  username        - account username for last-seen-ID tracking
 */
export async function extractDataFromProfile(
  browser,
  targetUrl,
  pgClient       = null,
  extractParents = true,
  hoursBack      = 0,
  fastMode       = true,
  maxScrolls     = DEFAULT_MAX_SCROLLS,
  username       = null
) {
  const page = await browser.newPage();
  let result = { links: [], parentLinks: [], title: 'Error Page' };

  try {
    console.log('🔑 Loading session...');
    await loadSession(page);

    const cutoffTime = await getCutoffTime(pgClient, hoursBack);

    // Load last-seen tweet ID for incremental extraction
    const lastSeenTweetId = username
      ? await getLastSeenTweetId(pgClient, username)
      : null;

    // Navigate to Live Search with cache-bust headers
    await navigateAndForceRefresh(page, targetUrl);

    // ── UNIFIED SCROLL: cutoff + last-seen ID + empty-load detection ─────────
    const { newestTweetIdSeen } = await scrollUntilCutoff(
      page,
      cutoffTime,
      lastSeenTweetId,
      maxScrolls,
      BETWEEN_SCROLL_DELAY_MS
    );

    // Extract all tweets from DOM
    const profileLinks = await extractLinksAndParents(page, cutoffTime);

    // Save newest tweet ID for this account so next run can stop early
    if (username && newestTweetIdSeen) {
      // Only advance the pointer if this ID is newer than what was stored
      const storedId = lastSeenTweetId;
      if (!storedId || BigInt(newestTweetIdSeen) > BigInt(storedId)) {
        await saveLastSeenTweetId(pgClient, username, newestTweetIdSeen);
      }
    }

    // ── ALWAYS extract parent tweets for every reply ──────────────────────
    const parentLinks = await extractAllParentTweets(browser, profileLinks, cutoffTime);

    const title = await page.title() || 'Untitled';

    console.log('');
    console.log('═'.repeat(80));
    console.log('✅ EXTRACTION COMPLETE');
    console.log(`   Mode:            ${hoursBack > 0 ? `LAST ${hoursBack} HOURS` : 'FULL HISTORY'}`);
    console.log(`   Source:          Live Search`);
    console.log(`   Max scrolls:     ${maxScrolls}`);
    console.log(`   Profile links:   ${profileLinks.length}`);
    console.log(`   Parent links:    ${parentLinks.length}`);
    console.log(`   Original URL:    ${targetUrl}`);
    console.log('═'.repeat(80));

    result = { links: profileLinks, parentLinks, title };

  } catch (err) {
    console.error(`\n❌ EXTRACTION ERROR: ${err.message}`);
    console.error(`   URL: ${targetUrl}\n`);
  } finally {
    try { await saveSession(page); } catch (e) { console.warn('⚠️  saveSession:', e.message); }
    try { await page.close();    } catch (e) { console.warn('⚠️  page.close:',   e.message); }
  }

  return result;
}

export async function extractParentTweetsInParallel() {
  console.warn('⚠️ Use extractDataFromProfile with extractParents=true instead');
  return new Map();
}
