// ==============================================================================
// EXECUTION WORKFLOWS COLLECTION
// ==============================================================================

console.log('Creating execution_workflows collection...');

db.createCollection("execution_workflows", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["workflow_name", "workflow_type", "created_at", "is_active"],
      properties: {
        workflow_name: {
          bsonType: "string",
          description: "Unique name for the execution workflow"
        },
        workflow_type: {
          bsonType: "string",
          enum: [
            "daily", "weekly", "monthly", "custom",
            "reply", "message", "retweet", "post",
            "automated", "manual", "scheduled", "batch"
          ],
          description: "Type of execution workflow"
        },
        description: {
          bsonType: ["string", "null"],
          description: "Description of the workflow purpose"
        },
        content_type: {
          bsonType: "string",
          description: "Type of content this workflow processes"
        },
        template_source_id: {
          bsonType: ["string", "null"],
          description: "ID of the template workflow this was created from"
        },
        template_source_name: {
          bsonType: ["string", "null"],
          description: "Name of the template workflow this was created from"
        },
        automa_config: {
          bsonType: ["object", "null"],
          description: "Automa workflow configuration"
        },
        destination_settings: {
          bsonType: ["object", "null"],
          description: "Destination settings for this workflow"
        },
        execution_settings: {
          bsonType: "object",
          description: "Execution timing and delay settings",
          properties: {
            delays: {
              bsonType: "object",
              properties: {
                count: { bsonType: "int", minimum: 1, maximum: 10 },
                min_seconds: { bsonType: "double", minimum: 0.1, maximum: 60.0 },
                max_seconds: { bsonType: "double", minimum: 0.1, maximum: 120.0 }
              }
            },
            press_keys: {
              bsonType: "object",
              properties: {
                min_seconds: { bsonType: "double", minimum: 0.1, maximum: 10.0 },
                max_seconds: { bsonType: "double", minimum: 0.1, maximum: 20.0 }
              }
            },
            click_elements: {
              bsonType: "object",
              properties: {
                min_seconds: { bsonType: "double", minimum: 0.1, maximum: 10.0 },
                max_seconds: { bsonType: "double", minimum: 0.1, maximum: 20.0 }
              }
            }
          }
        },
        schedule_config: {
          bsonType: ["object", "null"],
          description: "Schedule configuration for this workflow",
          properties: {
            day_key: { bsonType: "string" },
            day_name: { bsonType: "string" },
            scheduled_date: { bsonType: "string" },
            recurring: { bsonType: "bool" },
            recurrence_pattern: { bsonType: "string" }
          }
        },
        account_specific: {
          bsonType: ["bool", "null"],
          description: "Whether this workflow is account-specific"
        },
        postgres_account_id: {
          bsonType: ["int", "null"],
          description: "PostgreSQL account ID if workflow is account-specific"
        },
        account_username: {
          bsonType: ["string", "null"],
          description: "Account username if workflow is account-specific"
        },
        is_active: {
          bsonType: "bool",
          description: "Whether the workflow is active"
        },
        execution_count: {
          bsonType: "int",
          minimum: 0,
          description: "Number of times this workflow has been executed"
        },
        last_executed_at: {
          bsonType: ["date", "null"],
          description: "When this workflow was last executed"
        },
        last_successful_execution: {
          bsonType: ["date", "null"],
          description: "When this workflow last executed successfully"
        },
        success_rate: {
          bsonType: "double",
          minimum: 0,
          maximum: 100,
          description: "Success rate percentage"
        },
        tags: {
          bsonType: ["array", "null"],
          description: "Tags for categorizing workflows",
          items: {
            bsonType: "string"
          }
        },
        metadata: {
          bsonType: ["object", "null"],
          description: "Additional metadata for the workflow"
        },
        created_by: {
          bsonType: ["string", "null"],
          description: "User/system that created this workflow"
        },
        created_at: {
          bsonType: "date",
          description: "When this workflow was created"
        },
        updated_at: {
          bsonType: "date",
          description: "When this workflow was last updated"
        }
      }
    }
  }
});

// Create indexes
db.execution_workflows.createIndex({ "workflow_name": 1 }, { unique: true });
db.execution_workflows.createIndex({ "workflow_type": 1 });
db.execution_workflows.createIndex({ "content_type": 1 });
db.execution_workflows.createIndex({ "is_active": 1 });
db.execution_workflows.createIndex({ "postgres_account_id": 1 });
db.execution_workflows.createIndex({ "created_at": -1 });
db.execution_workflows.createIndex({ "last_executed_at": -1 });
db.execution_workflows.createIndex({ "workflow_type": 1, "content_type": 1 });
db.execution_workflows.createIndex({ "tags": 1 });

console.log('✅ Created execution_workflows collection with indexes');

// ==============================================================================
// CREATE VIEWS FOR EXECUTION WORKFLOWS
// ==============================================================================

console.log('Creating execution workflows views...');

// View for active execution workflows
try {
  db.createView("active_execution_workflows", "execution_workflows", [
    {
      $match: {
        is_active: true
      }
    },
    {
      $project: {
        workflow_name: 1,
        workflow_type: 1,
        description: 1,
        content_type: 1,
        template_source_name: 1,
        execution_settings: 1,
        schedule_config: 1,
        account_specific: 1,
        postgres_account_id: 1,
        account_username: 1,
        execution_count: 1,
        last_executed_at: 1,
        success_rate: 1,
        tags: 1,
        created_at: 1,
        updated_at: 1
      }
    },
    {
      $sort: { workflow_name: 1 }
    }
  ]);
  console.log('✓ Created active_execution_workflows view');
} catch (e) {
  console.log("active_execution_workflows view already exists or creation failed:", e.message);
}

// View for workflow statistics by type
try {
  db.createView("workflow_type_statistics", "execution_workflows", [
    {
      $group: {
        _id: "$workflow_type",
        total_workflows: { $sum: 1 },
        active_workflows: {
          $sum: { $cond: [{ $eq: ["$is_active", true] }, 1, 0] }
        },
        average_execution_count: { $avg: "$execution_count" },
        average_success_rate: { $avg: "$success_rate" },
        unique_content_types: { $addToSet: "$content_type" },
        last_created: { $max: "$created_at" },
        last_executed: { $max: "$last_executed_at" }
      }
    },
    {
      $addFields: {
        content_type_count: { $size: "$unique_content_types" }
      }
    },
    {
      $project: {
        workflow_type: "$_id",
        total_workflows: 1,
        active_workflows: 1,
        inactive_workflows: { $subtract: ["$total_workflows", "$active_workflows"] },
        average_execution_count: { $round: ["$average_execution_count", 2] },
        average_success_rate: { $round: ["$average_success_rate", 2] },
        content_type_count: 1,
        unique_content_types: 1,
        last_created: 1,
        last_executed: 1,
        _id: 0
      }
    },
    {
      $sort: { total_workflows: -1 }
    }
  ]);
  console.log('✓ Created workflow_type_statistics view');
} catch (e) {
  console.log("workflow_type_statistics view already exists or creation failed:", e.message);
}

// View for account-specific workflows
try {
  db.createView("account_specific_workflows", "execution_workflows", [
    {
      $match: {
        account_specific: true,
        postgres_account_id: { $ne: null }
      }
    },
    {
      $lookup: {
        from: "accounts",
        localField: "postgres_account_id",
        foreignField: "postgres_account_id",
        as: "account_info"
      }
    },
    {
      $unwind: {
        path: "$account_info",
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $project: {
        workflow_name: 1,
        workflow_type: 1,
        description: 1,
        content_type: 1,
        postgres_account_id: 1,
        account_username: 1,
        account_profile_id: "$account_info.profile_id",
        account_created_time: "$account_info.created_time",
        execution_count: 1,
        last_executed_at: 1,
        success_rate: 1,
        is_active: 1,
        created_at: 1,
        updated_at: 1
      }
    },
    {
      $sort: { postgres_account_id: 1, workflow_name: 1 }
    }
  ]);
  console.log('✓ Created account_specific_workflows view');
} catch (e) {
  console.log("account_specific_workflows view already exists or creation failed:", e.message);
}

// View for workflow execution history
try {
  db.createView("workflow_execution_history", "execution_sessions", [
    {
      $unwind: {
        path: "$workflows",
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $match: {
        "workflows.workflow_name": { $exists: true, $ne: null }
      }
    },
    {
      $lookup: {
        from: "execution_workflows",
        localField: "workflows.workflow_name",
        foreignField: "workflow_name",
        as: "workflow_info"
      }
    },
    {
      $unwind: {
        path: "$workflow_info",
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $group: {
        _id: {
          workflow_name: "$workflows.workflow_name",
          execution_date: {
            $dateToString: { format: "%Y-%m-%d", date: "$created_at" }
          }
        },
        workflow_type: { $first: "$workflow_info.workflow_type" },
        content_type: { $first: "$workflow_info.content_type" },
        execution_count: { $sum: 1 },
        successful_executions: {
          $sum: { $cond: [{ $eq: ["$workflows.success", true] }, 1, 0] }
        },
        failed_executions: {
          $sum: { $cond: [{ $eq: ["$workflows.success", false] }, 1, 0] }
        },
        total_execution_time_seconds: { $sum: "$workflows.execution_time" },
        unique_accounts: { $addToSet: "$postgres_account_id" },
        last_execution_time: { $max: "$created_at" }
      }
    },
    {
      $addFields: {
        success_rate: {
          $cond: [
            { $gt: ["$execution_count", 0] },
            {
              $multiply: [
                { $divide: ["$successful_executions", "$execution_count"] },
                100
              ]
            },
            0
          ]
        },
        avg_execution_time_seconds: {
          $cond: [
            { $gt: ["$execution_count", 0] },
            { $divide: ["$total_execution_time_seconds", "$execution_count"] },
            0
          ]
        },
        account_count: { $size: "$unique_accounts" }
      }
    },
    {
      $project: {
        workflow_name: "$_id.workflow_name",
        execution_date: "$_id.execution_date",
        workflow_type: 1,
        content_type: 1,
        execution_count: 1,
        successful_executions: 1,
        failed_executions: 1,
        success_rate: { $round: ["$success_rate", 2] },
        avg_execution_time_seconds: { $round: ["$avg_execution_time_seconds", 2] },
        account_count: 1,
        last_execution_time: 1,
        _id: 0
      }
    },
    {
      $sort: { execution_date: -1, workflow_name: 1 }
    }
  ]);
  console.log('✓ Created workflow_execution_history view');
} catch (e) {
  console.log("workflow_execution_history view already exists or creation failed:", e.message);
}

// ==============================================================================
// INSERT SAMPLE EXECUTION WORKFLOWS
// ==============================================================================

console.log('Inserting sample execution workflows...');

const sampleExecutionWorkflows = [
  {
    workflow_name: "daily_replies_workflow",
    workflow_type: "daily",
    description: "Daily workflow for processing reply content",
    content_type: "replies",
    template_source_id: null,
    template_source_name: null,
    destination_settings: {
      collection_name: "execution_workflows",
      storage_format: "json"
    },
    execution_settings: {
      delays: { count: 2, min_seconds: 1.0, max_seconds: 5.0 },
      press_keys: { min_seconds: 0.5, max_seconds: 2.0 },
      click_elements: { min_seconds: 0.3, max_seconds: 1.5 }
    },
    schedule_config: {
      day_key: "monday",
      day_name: "Monday",
      scheduled_date: new Date().toISOString(),
      recurring: true,
      recurrence_pattern: "daily"
    },
    account_specific: false,
    postgres_account_id: null,
    account_username: null,
    is_active: true,
    execution_count: 0,
    last_executed_at: null,
    last_successful_execution: null,
    success_rate: 0,
    tags: ["replies", "daily", "automated"],
    metadata: {
      version: "1.0.0",
      created_by_ui: true
    },
    created_by: "system",
    created_at: new Date(),
    updated_at: new Date()
  },
  {
    workflow_name: "weekly_messages_batch",
    workflow_type: "weekly",
    description: "Weekly batch workflow for processing messages",
    content_type: "messages",
    template_source_id: null,
    template_source_name: null,
    destination_settings: {
      collection_name: "execution_workflows",
      storage_format: "json"
    },
    execution_settings: {
      delays: { count: 3, min_seconds: 2.0, max_seconds: 8.0 },
      press_keys: { min_seconds: 0.8, max_seconds: 3.0 },
      click_elements: { min_seconds: 0.5, max_seconds: 2.0 }
    },
    schedule_config: {
      day_key: "wednesday",
      day_name: "Wednesday",
      scheduled_date: new Date().toISOString(),
      recurring: true,
      recurrence_pattern: "weekly"
    },
    account_specific: false,
    postgres_account_id: null,
    account_username: null,
    is_active: true,
    execution_count: 0,
    last_executed_at: null,
    last_successful_execution: null,
    success_rate: 0,
    tags: ["messages", "weekly", "batch"],
    metadata: {
      version: "1.0.0",
      created_by_ui: true
    },
    created_by: "system",
    created_at: new Date(),
    updated_at: new Date()
  },
  {
    workflow_name: "custom_retweet_automation",
    workflow_type: "custom",
    description: "Custom automation for retweet processing",
    content_type: "retweets",
    template_source_id: null,
    template_source_name: null,
    destination_settings: {
      collection_name: "execution_workflows",
      storage_format: "json"
    },
    execution_settings: {
      delays: { count: 1, min_seconds: 0.5, max_seconds: 3.0 },
      press_keys: { min_seconds: 0.3, max_seconds: 1.5 },
      click_elements: { min_seconds: 0.2, max_seconds: 1.0 }
    },
    schedule_config: {
      day_key: "friday",
      day_name: "Friday",
      scheduled_date: new Date().toISOString(),
      recurring: false,
      recurrence_pattern: null
    },
    account_specific: true,
    postgres_account_id: 1,
    account_username: "sample_user",
    is_active: true,
    execution_count: 5,
    last_executed_at: new Date(Date.now() - 86400000), // 1 day ago
    last_successful_execution: new Date(Date.now() - 86400000),
    success_rate: 80.0,
    tags: ["retweets", "custom", "account_specific"],
    metadata: {
      version: "1.0.0",
      account_id: 1,
      created_by_ui: true
    },
    created_by: "admin",
    created_at: new Date(Date.now() - 7 * 86400000), // 7 days ago
    updated_at: new Date()
  }
];

// Insert sample workflows
sampleExecutionWorkflows.forEach(workflow => {
  try {
    db.execution_workflows.updateOne(
      { workflow_name: workflow.workflow_name },
      { $setOnInsert: workflow },
      { upsert: true }
    );
  } catch (error) {
    console.log(`Error inserting workflow ${workflow.workflow_name}: ${error.message}`);
  }
});

console.log(`✅ Inserted ${sampleExecutionWorkflows.length} sample execution workflows`);

// ==============================================================================
// UPDATE SYSTEM SETTINGS FOR EXECUTION WORKFLOWS
// ==============================================================================

console.log('Updating system settings for execution workflows...');

try {
  const settingsUpdate = db.settings.updateOne(
    { "category": "system" },
    {
      $set: {
        "settings.execution_workflow_settings": {
          "enabled": true,
          "auto_create_from_templates": true,
          "default_workflow_type": "daily",
          "default_execution_settings": {
            "delays": { "count": 2, "min_seconds": 1.0, "max_seconds": 5.0 },
            "press_keys": { "min_seconds": 0.5, "max_seconds": 2.0 },
            "click_elements": { "min_seconds": 0.3, "max_seconds": 1.5 }
          },
          "name_generation_strategy": "template_content_date",
          "name_templates": {
            "daily": "{content_type}_daily_{date}",
            "weekly": "{content_type}_weekly_{date}",
            "custom": "{content_type}_{template_name}_{date}"
          },
          "workflow_retention_days": 90,
          "archive_inactive_workflows": true,
          "max_workflows_per_account": 50,
          "default_tags": ["created_from_template", "automa"]
        },
        "updated_at": new Date()
      }
    },
    { upsert: true }
  );
  console.log('✅ Updated system settings with execution workflow configuration');
} catch (error) {
  console.log(`Error updating system settings: ${error.message}`);
}

// ==============================================================================
// CREATE TRIGGER FOR WORKFLOW EXECUTION COUNTING
// ==============================================================================

console.log('Creating database functions for workflow execution tracking...');

// Function to increment execution count
try {
  db.system.js.save({
    _id: "incrementWorkflowExecutionCount",
    value: function(workflowName, success) {
      const workflow = db.execution_workflows.findOne({ workflow_name: workflowName });

      if (!workflow) {
        return { success: false, message: "Workflow not found" };
      }

      const update = {
        $inc: { execution_count: 1 },
        $set: {
          last_executed_at: new Date(),
          updated_at: new Date()
        }
      };

      if (success) {
        update.$set.last_successful_execution = new Date();
      }

      // Calculate new success rate
      const newTotal = workflow.execution_count + 1;
      const newSuccessCount = success
        ? (workflow.success_rate * workflow.execution_count / 100) + 1
        : (workflow.success_rate * workflow.execution_count / 100);

      update.$set.success_rate = (newSuccessCount / newTotal) * 100;

      const result = db.execution_workflows.updateOne(
        { workflow_name: workflowName },
        update
      );

      return {
        success: result.modifiedCount > 0,
        updatedWorkflow: db.execution_workflows.findOne({ workflow_name: workflowName })
      };
    }
  });
} catch (error) {
  console.log(`Error creating incrementWorkflowExecutionCount function: ${error.message}`);
}

// Function to get workflow statistics
try {
  db.system.js.save({
    _id: "getWorkflowStatistics",
    value: function(days = 30) {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - days);

      const pipeline = [
        {
          $match: {
            created_at: { $gte: cutoffDate }
          }
        },
        {
          $group: {
            _id: "$workflow_type",
            total_workflows: { $sum: 1 },
            active_workflows: { $sum: { $cond: [{ $eq: ["$is_active", true] }, 1, 0] } },
            total_executions: { $sum: "$execution_count" },
            avg_success_rate: { $avg: "$success_rate" },
            recent_workflows: {
              $sum: {
                $cond: [
                  { $gte: ["$created_at", new Date(Date.now() - 7 * 86400000)] },
                  1,
                  0
                ]
              }
            }
          }
        },
        {
          $project: {
            workflow_type: "$_id",
            total_workflows: 1,
            active_workflows: 1,
            inactive_workflows: { $subtract: ["$total_workflows", "$active_workflows"] },
            total_executions: 1,
            avg_success_rate: { $round: ["$avg_success_rate", 2] },
            recent_workflows: 1,
            _id: 0
          }
        },
        {
          $sort: { total_workflows: -1 }
        }
      ];

      return db.execution_workflows.aggregate(pipeline).toArray();
    }
  });
  console.log('✓ Created workflow execution tracking functions');
} catch (error) {
  console.log(`Error creating getWorkflowStatistics function: ${error.message}`);
}

// ==============================================================================
// FINAL VALIDATION
// ==============================================================================

console.log('Running final validation...');

// Validate collection exists
try {
  const collectionExists = db.getCollectionNames().includes("execution_workflows");
  console.log(`- execution_workflows collection exists: ${collectionExists ? '✅' : '❌'}`);

  // Check document count
  const docCount = db.execution_workflows.countDocuments();
  console.log(`- Documents in collection: ${docCount}`);

  // Check indexes
  const indexes = db.execution_workflows.getIndexes();
  console.log(`- Number of indexes: ${indexes.length}`);

  // Test sample workflow retrieval
  const sampleWorkflow = db.execution_workflows.findOne({ workflow_name: "daily_replies_workflow" });
  console.log(`- Sample workflow found: ${sampleWorkflow ? '✅' : '❌'}`);

  // Test view creation
  try {
    const viewResult = db.active_execution_workflows.find().limit(1).toArray();
    console.log(`- Views are functional: ${viewResult ? '✅' : '❌'}`);
  } catch (e) {
    console.log(`- Views test failed: ${e.message}`);
  }
} catch (error) {
  console.log(`Validation error: ${error.message}`);
}

console.log('');
console.log('=== EXECUTION WORKFLOWS SCHEMA SETUP COMPLETE ===');
console.log('');
console.log('Summary:');
console.log('- Created execution_workflows collection with comprehensive schema');
console.log('- Added 4 views for different perspectives on workflow data');
console.log('- Inserted sample workflows for testing');
console.log('- Updated system settings with execution workflow configuration');
console.log('- Created database functions for execution tracking');
console.log('');
console.log('Key features:');
console.log('1. Workflow name uniqueness enforcement');
console.log('2. Comprehensive type system (daily, weekly, custom, etc.)');
console.log('3. Account-specific workflow tracking');
console.log('4. Execution statistics and success rate tracking');
console.log('5. Schedule configuration support');
console.log('6. Template-to-execution workflow linkage');
console.log('7. Rich metadata and tagging support');
console.log('');
console.log('Ready for use in:');
console.log('- Automa Workflow Configuration UI');
console.log('- Workflow execution tracking');
console.log('- Account-specific workflow management');
console.log('- Analytics and reporting');
