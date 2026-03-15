import { URL } from 'url';
import { MongoClient } from 'mongodb';

// Function to parse DATABASE_URL
export function parseDatabaseUrl(databaseUrl) {
    if (!databaseUrl) {
        return null;
    }

    try {
        const url = new URL(databaseUrl);
        return {
            user: url.username,
            password: url.password,
            host: url.hostname,
            port: parseInt(url.port) || 5432,
            database: url.pathname.substring(1)
        };
    } catch (error) {
        console.error('❌ Error parsing DATABASE_URL:', error.message);
        return null;
    }
}

// Function to extract tweet ID from X.com URLs
export function extractTweetId(url) {
    try {
        const match = url.match(/\/status\/(\d+)/);
        return match ? match[1] : null;
    } catch (error) {
        return null;
    }
}

// Function to get canonical tweet URL
export function getCanonicalTweetUrl(url) {
    try {
        const tweetId = extractTweetId(url);
        if (!tweetId) return url;

        const usernameMatch = url.match(/https?:\/\/x\.com\/([^\/]+)\/status/);
        if (!usernameMatch) return url;

        const username = usernameMatch[1];
        return `https://x.com/${username}/status/${tweetId}`;
    } catch (error) {
        return url;
    }
}

// Function to get filter words from MongoDB settings
export async function getFilterWordsFromMongo() {
    const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin';

    try {
        const mongoClient = new MongoClient(MONGODB_URI);
        await mongoClient.connect();
        console.log('✅ Connected to MongoDB for filter words');

        const db = mongoClient.db('messages_db');
        const settingsDoc = await db.collection('settings').findOne({ category: 'system' });

        let filterWords = [];

        if (settingsDoc && settingsDoc.settings && settingsDoc.settings.extraction_processing_settings) {
            const wordsToFilter = settingsDoc.settings.extraction_processing_settings.words_to_filter;
            if (wordsToFilter) {
                filterWords = wordsToFilter.split(',').map(word => word.trim().toLowerCase());
                console.log(`✅ Retrieved ${filterWords.length} filter words from MongoDB`);
            }
        }

        await mongoClient.close();

        // If no words found in MongoDB, use defaults
        if (filterWords.length === 0) {
            const defaultWords = 'advert,advertisement,sponsored,promo,promotion,spam,inappropriate,blocked,offensive,hate';
            filterWords = defaultWords.split(',').map(word => word.trim().toLowerCase());
            console.log(`⚠️ Using default filter words (${filterWords.length} words)`);
        }

        return filterWords;

    } catch (error) {
        console.error('❌ Error getting filter words from MongoDB:', error.message);

        // Fallback to default words
        const defaultWords = 'advert,advertisement,sponsored,promo,promotion,spam,inappropriate,blocked,offensive,hate';
        const filterWords = defaultWords.split(',').map(word => word.trim().toLowerCase());
        console.log(`⚠️ Using fallback filter words (${filterWords.length} words)`);

        return filterWords;
    }
}

// Function to check if text contains any filter words
// Enhanced filtering logic for utils.js
export function containsFilterWords(text, filterWords) {
    if (!text || !Array.isArray(filterWords) || filterWords.length === 0) {
        return false;
    }

    const textLower = text.toLowerCase();
    return filterWords.some(word => {
        if (!word || word.trim() === '') return false;
        return textLower.includes(word.toLowerCase());
    });
}

// New function to check entire URL for filter words
export function urlContainsFilterWords(url, filterWords) {
    if (!url || !Array.isArray(filterWords) || filterWords.length === 0) {
        return false;
    }

    // Check the entire URL for filter words
    const urlLower = url.toLowerCase();
    return filterWords.some(word => {
        if (!word || word.trim() === '') return false;
        return urlLower.includes(word.toLowerCase());
    });
}

// Function to validate tweet ID format
export function isValidTweetId(tweetId) {
    return /^\d+$/.test(tweetId);
}

// Function to check if tweet ID contains specific pattern (like "19")
export function tweetIdContainsPattern(tweetId, pattern) {
    return tweetId && tweetId.includes(pattern);
}

export function extractTweetTimestampFromId(tweetId) {
    if (!tweetId || typeof tweetId !== 'string' || !/^\d+$/.test(tweetId)) {
        return null;
    }

    try {
        // Twitter Snowflake ID structure:
        // - First 41 bits: timestamp (milliseconds since Twitter epoch)
        // - Next 10 bits: datacenter ID
        // - Next 12 bits: worker ID
        // - Last 12 bits: sequence number

        const SNOWFLAKE_EPOCH = 1288834974657; // Twitter epoch (Nov 04, 2010 01:42:54 UTC)
        const id = BigInt(tweetId);

        // Shift right by 22 bits to get the timestamp, then add Twitter epoch
        const timestampMs = Number((id >> 22n) + BigInt(SNOWFLAKE_EPOCH));

        // Create Date object
        const date = new Date(timestampMs);

        // Validate the date (should be between 2010 and now+1 year for sanity check)
        const currentYear = new Date().getFullYear();
        if (date.getFullYear() < 2010 || date.getFullYear() > currentYear + 1) {
            console.warn(`Invalid tweet date extracted: ${date.toISOString()} for tweet ${tweetId}`);
            return null;
        }

        return {
            timestamp: Math.floor(timestampMs / 1000), // Convert to seconds
            timestampMs: timestampMs,
            date: date,
            iso: date.toISOString(),
            year: date.getFullYear(),
            month: date.getMonth() + 1,
            day: date.getDate(),
            hour: date.getHours(),
            minute: date.getMinutes(),
            second: date.getSeconds()
        };
    } catch (error) {
        console.error(`Error extracting timestamp from tweet ID ${tweetId}:`, error);
        return null;
    }
}
