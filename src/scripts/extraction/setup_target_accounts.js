// scripts/setup_target_accounts.js
import { MongoClient } from 'mongodb';

async function setupTargetAccounts() {
  if (!process.env.MONGODB_URI) {
    console.error('❌ MONGODB_URI environment variable not set');
    process.exit(1);
  }

  const mongoClient = new MongoClient(process.env.MONGODB_URI);

  try {
    await mongoClient.connect();
    const db = mongoClient.db('messages_db');

    const collection = db.collection('target_accounts');
    await collection.deleteMany({});

    // ALWAYS use individual profile URLs with /with_replies
    const accounts = [
      {
        username: 'Eileavelancia',
        display_name: 'eileen',
        url: 'https://x.com/Eileavelancia/with_replies', // INDIVIDUAL PROFILE
        account_id: 1,
        priority: 1,
        active: true,
        notes: 'Primary account - individual profile',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        username: 'Record_spot1',
        display_name: 'ASSIGNMENTS/ESSAY...',
        url: 'https://x.com/Record_spot1/with_replies', // INDIVIDUAL PROFILE
        account_id: 2,
        priority: 2,
        active: true,
        notes: 'Assignment help - individual profile',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        username: 'brill_writer',
        display_name: 'The Brilliant Writer',
        url: 'https://x.com/brill_writer/with_replies', // INDIVIDUAL PROFILE
        account_id: 3,
        priority: 3,
        active: true,
        notes: 'Homework help - individual profile',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        username: 'essaypro',
        display_name: 'essayzpro',
        url: 'https://x.com/essaypro/with_replies', // INDIVIDUAL PROFILE
        account_id: 4,
        priority: 4,
        active: true,
        notes: 'Essay help - individual profile',
        created_at: new Date(),
        updated_at: new Date()
      }
    ];

    const result = await collection.insertMany(accounts);
    console.log(`✅ Inserted ${result.insertedCount} target accounts`);

    console.log('\n📋 Accounts configured for INDIVIDUAL PROFILE extraction:');
    console.log('─'.repeat(80));
    accounts.forEach(acc => {
      console.log(`@${acc.username} -> ${acc.url}`);
    });
    console.log('─'.repeat(80));
    console.log('\n✅ All accounts use individual profile pages (/with_replies)');

  } catch (error) {
    console.error('❌ Setup failed:', error);
    process.exit(1);
  } finally {
    await mongoClient.close();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  setupTargetAccounts().catch(console.error);
}

export { setupTargetAccounts };
