// MongoDB Initialization Script for Workflow Management System
// Run this script using: mongosh < init_mongodb.js

// Connect to your database (adjust database name as needed)
db = db.getSiblingDB('workflow_db');

print('Starting MongoDB initialization...');

// ===================================================================
// 1. Create Collections
// ===================================================================

print('\n1. Creating collections...');

// Create automa_workflows collection for pure Automa workflow data
db.createCollection('automa_workflows');
print('✓ Created automa_workflows collection');

// Create workflow_metadata collection for unified metadata
db.createCollection('workflow_metadata');
print('✓ Created workflow_metadata collection');

// ===================================================================
// 2. Create Indexes for workflow_metadata
// ===================================================================

print('\n2. Creating indexes for workflow_metadata...');

// Primary lookup indexes
db.workflow_metadata.createIndex(
  { automa_workflow_id: 1 },
  { unique: true, name: 'idx_automa_workflow_id' }
);
print('✓ Created unique index on automa_workflow_id');

db.workflow_metadata.createIndex(
  { workflow_type: 1, has_content: 1 },
  { name: 'idx_workflow_type_has_content' }
);
print('✓ Created compound index on workflow_type and has_content');

db.workflow_metadata.createIndex(
  { postgres_content_id: 1, workflow_type: 1 },
  { name: 'idx_postgres_content_id_workflow_type' }
);
print('✓ Created compound index on postgres_content_id and workflow_type');

db.workflow_metadata.createIndex(
  { postgres_account_id: 1 },
  { name: 'idx_postgres_account_id' }
);
print('✓ Created index on postgres_account_id');

// Execution-related indexes
db.workflow_metadata.createIndex(
  { execute: 1, executed: 1, has_content: 1 },
  { name: 'idx_execution_ready' }
);
print('✓ Created compound index for execution readiness');

db.workflow_metadata.createIndex(
  { status: 1, created_at: -1 },
  { name: 'idx_status_created_at' }
);
print('✓ Created compound index on status and created_at');

// Time-based indexes
db.workflow_metadata.createIndex(
  { created_at: -1 },
  { name: 'idx_created_at_desc' }
);
print('✓ Created descending index on created_at');

db.workflow_metadata.createIndex(
  { executed_at: -1 },
  { sparse: true, name: 'idx_executed_at_desc' }
);
print('✓ Created sparse index on executed_at');

// Reference indexes
db.workflow_metadata.createIndex(
  { execution_id: 1 },
  { name: 'idx_execution_id' }
);
print('✓ Created index on execution_id');

db.workflow_metadata.createIndex(
  { username: 1 },
  { sparse: true, name: 'idx_username' }
);
print('✓ Created sparse index on username');

// Priority and performance indexes
db.workflow_metadata.createIndex(
  { processing_priority: 1, created_at: 1 },
  { name: 'idx_priority_created' }
);
print('✓ Created compound index on processing_priority and created_at');

// ===================================================================
// 3. Create Indexes for automa_workflows
// ===================================================================

print('\n3. Creating indexes for automa_workflows...');

db.automa_workflows.createIndex(
  { name: 1 },
  { name: 'idx_workflow_name' }
);
print('✓ Created index on workflow name');

db.automa_workflows.createIndex(
  { workflow_type: 1 },
  { sparse: true, name: 'idx_workflow_type' }
);
print('✓ Created sparse index on workflow_type');

// ===================================================================
// 4. Set up Validation Rules
// ===================================================================

print('\n4. Setting up validation rules...');

// ===================================================================
// 4. Setting up validation rules...
// ===================================================================

print('\n4. Setting up validation rules...');

db.runCommand({
  collMod: 'workflow_metadata',
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: [
        'automa_workflow_id',
        'workflow_type',
        'workflow_name',
        'execution_id',
        'status',
        'has_content',
        'execute',
        'executed',
        'success',
        'created_at',
        'updated_at'
      ],
      properties: {
        automa_workflow_id: {
          bsonType: 'objectId',
          description: 'Reference to automa_workflows collection'
        },
        workflow_type: {
          enum: ['replies', 'messages', 'retweets'],
          description: 'Type of workflow - must be replies, messages, or retweets'
        },
        workflow_name: { bsonType: 'string' },
        execution_id: { bsonType: 'string' },
        postgres_content_id: { bsonType: ['int', 'long', 'null'] },
        postgres_account_id: { bsonType: ['int', 'long', 'null'] },
        postgres_prompt_id: { bsonType: ['int', 'long', 'null'] },
        postgres_workflow_id: { bsonType: ['int', 'long', 'null'] },
        mongo_account_id: { bsonType: ['objectId', 'null'] },
        mongo_prompt_id: { bsonType: ['objectId', 'null'] },
        mongo_workflow_id: { bsonType: ['objectId', 'null'] },
        username: { bsonType: ['string', 'null'] },
        profile_id: { bsonType: ['string', 'null'] },
        account_id: { bsonType: ['int', 'long', 'null'] },
        assigned_url: { bsonType: ['string', 'null'] },
        assignment_method: { bsonType: ['string', 'null'] },
        content_preview: { bsonType: ['string', 'null'] },
        link_assigned_at: { bsonType: ['string', 'null'] },
        tweeted_date: { bsonType: ['string', 'null'] },
        content_text_preview: { bsonType: ['string', 'null'] },
        content_length: { bsonType: ['int', 'long', 'null'] },
        content_hash: { bsonType: ['long', 'null'] },
        has_link: { bsonType: 'bool' },
        link_url: { bsonType: ['string', 'null'] },
        status: {
          enum: [
            'generated',
            'ready_to_execute',
            'content_confirmed',
            'failed',
            'completed'
          ],
          description: 'Workflow status lifecycle'
        },
        has_content: { bsonType: 'bool' },
        execute: { bsonType: 'bool' },
        executed: { bsonType: 'bool' },
        success: { bsonType: 'bool' },
        execution_start: { bsonType: ['string', 'null'] },
        execution_end: { bsonType: ['string', 'null'] },
        execution_time_ms: { bsonType: ['int', 'long', 'null'] },
        generated_at: { bsonType: ['string', 'null'] },
        executed_at: { bsonType: ['string', 'null'] },
        content_status_updated_at: { bsonType: ['string', 'null'] },
        blocks_generated: { bsonType: ['int', 'long', 'null'] },
        template_used: { bsonType: ['string', 'null'] },
        processing_priority: { bsonType: ['int', 'long', 'null'] },
        generation_context: {
          bsonType: 'object',
          properties: {
            template_used: { bsonType: ['string', 'null'] },
            blocks_generated: { bsonType: ['int', 'long', 'null'] },
            placeholder_found: { bsonType: ['bool', 'null'] },
            account_based: { bsonType: ['bool', 'null'] },
            prompt_based: { bsonType: ['bool', 'null'] },
            workflow_based: { bsonType: ['bool', 'null'] },
            has_username: { bsonType: ['bool', 'null'] },
            has_profile: { bsonType: ['bool', 'null'] }
          }
        },
        performance_metrics: {
          bsonType: 'object',
          properties: {
            generation_time_ms: { bsonType: ['int', 'long', 'null'] },
            template_loading_successful: { bsonType: ['bool', 'null'] },
            placeholder_replacement_successful: { bsonType: ['bool', 'null'] },
            blocks_generated_count: { bsonType: ['int', 'long', 'null'] }
          }
        },
        error_message: { bsonType: ['string', 'null'] },
        execution_attempts: { bsonType: ['int', 'long', 'null'] },
        retry_count: { bsonType: ['int', 'long', 'null'] },
        last_error_message: { bsonType: ['string', 'null'] },
        last_error_timestamp: { bsonType: ['string', 'null'] },
        created_at: { bsonType: 'string' },
        updated_at: { bsonType: 'string' }
      }
    }
  },
  validationLevel: 'moderate',
  validationAction: 'warn'
});

print('✓ Updated validation rules for workflow_metadata');


// ===================================================================
// 5. Create Sample Documents (Optional)
// ===================================================================

print('\n5. Creating sample documents...');

// Insert a sample automa workflow
const sampleWorkflowId = db.automa_workflows.insertOne({
  version: '1.0',
  workflow_type: 'replies',
  name: 'sample_reply_workflow',
  description: 'Sample reply workflow for testing',
  created_at: new Date().toISOString(),
  drawflow: {
    nodes: []
  },
  settings: {},
  globalData: {}
}).insertedId;

print('✓ Created sample automa_workflow document');

// Insert a sample metadata record
db.workflow_metadata.insertOne({
  automa_workflow_id: sampleWorkflowId,
  workflow_type: 'replies',
  workflow_name: 'sample_reply_workflow',
  execution_id: '00000000-0000-0000-0000-000000000000',
  postgres_content_id: null,
  postgres_account_id: null,
  postgres_prompt_id: null,
  postgres_workflow_id: null,
  mongo_account_id: null,
  mongo_prompt_id: null,
  mongo_workflow_id: null,
  username: null,
  profile_id: null,
  content_text_preview: 'Sample content preview',
  content_length: 21,
  content_hash: null,
  has_link: false,
  link_url: null,
  status: 'generated',
  has_content: true,
  execute: false,
  executed: false,
  success: false,
  execution_start: new Date().toISOString(),
  execution_end: new Date().toISOString(),
  execution_time_ms: 100,
  generated_at: new Date().toISOString(),
  executed_at: null,
  content_status_updated_at: null,
  blocks_generated: 0,
  template_used: 'sample_template.json',
  processing_priority: 1,
  generation_context: {
    template_used: 'sample_template.json',
    blocks_generated: 0,
    placeholder_found: false
  },
  performance_metrics: {
    generation_time_ms: 100,
    template_loading_successful: true
  },
  error_message: null,
  execution_attempts: 0,
  retry_count: 0,
  last_error_message: null,
  last_error_timestamp: null,
  created_at: new Date().toISOString(),
  updated_at: new Date().toISOString()
});

print('✓ Created sample workflow_metadata document');

// ===================================================================
// 6. Display Collection Statistics
// ===================================================================

print('\n6. Collection statistics:');
print('─────────────────────────────────────────────────────────');

const automaStats = db.automa_workflows.stats();
print(`automa_workflows:`);
print(`  - Document count: ${db.automa_workflows.countDocuments()}`);
print(`  - Indexes: ${db.automa_workflows.getIndexes().length}`);

const metadataStats = db.workflow_metadata.stats();
print(`\nworkflow_metadata:`);
print(`  - Document count: ${db.workflow_metadata.countDocuments()}`);
print(`  - Indexes: ${db.workflow_metadata.getIndexes().length}`);

// ===================================================================
// 7. Display All Indexes
// ===================================================================

print('\n7. Created indexes:');
print('─────────────────────────────────────────────────────────');

print('\nautoma_workflows indexes:');
db.automa_workflows.getIndexes().forEach(idx => {
  print(`  - ${idx.name}: ${JSON.stringify(idx.key)}`);
});

print('\nworkflow_metadata indexes:');
db.workflow_metadata.getIndexes().forEach(idx => {
  print(`  - ${idx.name}: ${JSON.stringify(idx.key)}`);
});

print('\n✓ MongoDB initialization completed successfully!');
print('─────────────────────────────────────────────────────────\n');