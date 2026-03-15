#!/usr/bin/env mongosh
// MongoDB Migration: Weekly Settings Schema Enhancement
// File: migrations/mongodb/001_weekly_settings_schema.js
// Run with: mongosh --file migrations/mongodb/001_weekly_settings_schema.js

db = db.getSiblingDB('messages_db');

console.log('=== Starting Weekly Settings Schema Migration ===');

// ==============================================================================
// 1. CREATE WEEKLY_WORKFLOW_SCHEDULES COLLECTION
// ==============================================================================

console.log('1. Creating weekly_workflow_schedules collection...');

try {
  db.createCollection("weekly_workflow_schedules");
  
  // Create indexes for the weekly schedules collection
  db.weekly_workflow_schedules.createIndex({ "day_of_week": 1 });
  db.weekly_workflow_schedules.createIndex({ "is_enabled": 1 });
  db.weekly_workflow_schedules.createIndex({ "schedule_type": 1 });
  db.weekly_workflow_schedules.createIndex({ "created_at": -1 });
  
  console.log('✅ Created weekly_workflow_schedules collection with indexes');
} catch (e) {
  console.log("weekly_workflow_schedules collection may already exist:", e.message);
}

// ==============================================================================
// 2. INSERT DEFAULT WEEKLY SCHEDULE CONFIGURATION
// ==============================================================================

console.log('2. Inserting default weekly schedule configuration...');

const defaultWeeklySchedules = [
  {
    day_of_week: 'monday',
    day_name: 'Monday',
    is_enabled: true,
    schedule_type: 'weekday',
    morning_schedule: {
      enabled: true,
      time: '09:00',
      links_to_filter: 10,
      content_types: ['messages', 'replies'],
      gap_between_workflows_minutes: 5,
      time_limit_hours: 2
    },
    evening_schedule: {
      enabled: true,
      time: '18:00',
      links_to_filter: 10,
      content_types: ['messages', 'replies'],
      gap_between_workflows_minutes: 5,
      time_limit_hours: 2
    },
    daily_limits: {
      max_total_links: 20,
      max_total_time_hours: 4,
      max_sessions: 2
    },
    created_at: new Date(),
    updated_at: new Date(),
    migration_source: '001_weekly_settings_schema'
  },
  {
    day_of_week: 'tuesday',
    day_name: 'Tuesday',
    is_enabled: true,
    schedule_type: 'weekday',
    morning_schedule: {
      enabled: true,
      time: '09:00',
      links_to_filter: 10,
      content_types: ['messages', 'replies'],
      gap_between_workflows_minutes: 5,
      time_limit_hours: 2
    },
    evening_schedule: {
      enabled: true,
      time: '18:00',
      links_to_filter: 10,
      content_types: ['messages', 'replies'],
      gap_between_workflows_minutes: 5,
      time_limit_hours: 2
    },
    daily_limits: {
      max_total_links: 20,
      max_total_time_hours: 4,
      max_sessions: 2
    },
    created_at: new Date(),
    updated_at: new Date(),
    migration_source: '001_weekly_settings_schema'
  },
  {
    day_of_week: 'wednesday',
    day_name: 'Wednesday',
    is_enabled: true,
    schedule_type: 'weekday',
    morning_schedule: {
      enabled: true,
      time: '09:00',
      links_to_filter: 10,
      content_types: ['messages', 'replies'],
      gap_between_workflows_minutes: 5,
      time_limit_hours: 2
    },
    evening_schedule: {
      enabled: true,
      time: '18:00',
      links_to_filter: 10,
      content_types: ['messages', 'replies'],
      gap_between_workflows_minutes: 5,
      time_limit_hours: 2
    },
    daily_limits: {
      max_total_links: 20,
      max_total_time_hours: 4,
      max_sessions: 2
    },
    created_at: new Date(),
    updated_at: new Date(),
    migration_source: '001_weekly_settings_schema'
  },
  {
    day_of_week: 'thursday',
    day_name: 'Thursday',
    is_enabled: true,
    schedule_type: 'weekday',
    morning_schedule: {
      enabled: true,
      time: '09:00',
      links_to_filter: 10,
      content_types: ['messages', 'replies'],
      gap_between_workflows_minutes: 5,
      time_limit_hours: 2
    },
    evening_schedule: {
      enabled: true,
      time: '18:00',
      links_to_filter: 10,
      content_types: ['messages', 'replies'],
      gap_between_workflows_minutes: 5,
      time_limit_hours: 2
    },
    daily_limits: {
      max_total_links: 20,
      max_total_time_hours: 4,
      max_sessions: 2
    },
    created_at: new Date(),
    updated_at: new Date(),
    migration_source: '001_weekly_settings_schema'
  },
  {
    day_of_week: 'friday',
    day_name: 'Friday',
    is_enabled: true,
    schedule_type: 'weekday',
    morning_schedule: {
      enabled: true,
      time: '09:00',
      links_to_filter: 10,
      content_types: ['messages', 'replies'],
      gap_between_workflows_minutes: 5,
      time_limit_hours: 2
    },
    evening_schedule: {
      enabled: true,
      time: '18:00',
      links_to_filter: 10,
      content_types: ['messages', 'replies'],
      gap_between_workflows_minutes: 5,
      time_limit_hours: 2
    },
    daily_limits: {
      max_total_links: 20,
      max_total_time_hours: 4,
      max_sessions: 2
    },
    created_at: new Date(),
    updated_at: new Date(),
    migration_source: '001_weekly_settings_schema'
  },
  {
    day_of_week: 'saturday',
    day_name: 'Saturday',
    is_enabled: false,
    schedule_type: 'weekend',
    morning_schedule: {
      enabled: false,
      time: '10:00',
      links_to_filter: 5,
      content_types: ['messages'],
      gap_between_workflows_minutes: 10,
      time_limit_hours: 1
    },
    evening_schedule: {
      enabled: false,
      time: '17:00',
      links_to_filter: 5,
      content_types: ['messages'],
      gap_between_workflows_minutes: 10,
      time_limit_hours: 1
    },
    daily_limits: {
      max_total_links: 10,
      max_total_time_hours: 2,
      max_sessions: 2
    },
    created_at: new Date(),
    updated_at: new Date(),
    migration_source: '001_weekly_settings_schema'
  },
  {
    day_of_week: 'sunday',
    day_name: 'Sunday',
    is_enabled: false,
    schedule_type: 'weekend',
    morning_schedule: {
      enabled: false,
      time: '10:00',
      links_to_filter: 5,
      content_types: ['messages'],
      gap_between_workflows_minutes: 10,
      time_limit_hours: 1
    },
    evening_schedule: {
      enabled: false,
      time: '17:00',
      links_to_filter: 5,
      content_types: ['messages'],
      gap_between_workflows_minutes: 10,
      time_limit_hours: 1
    },
    daily_limits: {
      max_total_links: 10,
      max_total_time_hours: 2,
      max_sessions: 2
    },
    created_at: new Date(),
    updated_at: new Date(),
    migration_source: '001_weekly_settings_schema'
  }
];

// Insert default schedules if they don't exist
defaultWeeklySchedules.forEach(schedule => {
  try {
    const existingSchedule = db.weekly_workflow_schedules.findOne({
      day_of_week: schedule.day_of_week
    });
    
    if (!existingSchedule) {
      db.weekly_workflow_schedules.insertOne(schedule);
      console.log(`✅ Inserted default schedule for ${schedule.day_name}`);
    } else {
      console.log(`ℹ️  Schedule already exists for ${schedule.day_name}`);
    }
  } catch (e) {
    console.log(`❌ Error inserting schedule for ${schedule.day_name}: ${e.message}`);
  }
});

// ==============================================================================
// 3. UPDATE SETTINGS COLLECTION WITH WEEKLY CONFIGURATION
// ==============================================================================

console.log('3. Updating system settings with weekly configuration...');

const weeklyWorkflowSettings = {
  monday: {
    enabled: true,
    links_to_filter: 10,
    morning_time: '09:00',
    evening_time: '18:00',
    content_types: ['messages', 'replies'],
    gap_between_workflows: 300, // seconds
    time_limit: 2 // hours
  },
  tuesday: {
    enabled: true,
    links_to_filter: 10,
    morning_time: '09:00',
    evening_time: '18:00',
    content_types: ['messages', 'replies'],
    gap_between_workflows: 300,
    time_limit: 2
  },
  wednesday: {
    enabled: true,
    links_to_filter: 10,
    morning_time: '09:00',
    evening_time: '18:00',
    content_types: ['messages', 'replies'],
    gap_between_workflows: 300,
    time_limit: 2
  },
  thursday: {
    enabled: true,
    links_to_filter: 10,
    morning_time: '09:00',
    evening_time: '18:00',
    content_types: ['messages', 'replies'],
    gap_between_workflows: 300,
    time_limit: 2
  },
  friday: {
    enabled: true,
    links_to_filter: 10,
    morning_time: '09:00',
    evening_time: '18:00',
    content_types: ['messages', 'replies'],
    gap_between_workflows: 300,
    time_limit: 2
  },
  saturday: {
    enabled: false,
    links_to_filter: 5,
    morning_time: '10:00',
    evening_time: '17:00',
    content_types: ['messages'],
    gap_between_workflows: 600,
    time_limit: 1
  },
  sunday: {
    enabled: false,
    links_to_filter: 5,
    morning_time: '10:00',
    evening_time: '17:00',
    content_types: ['messages'],
    gap_between_workflows: 600,
    time_limit: 1
  }
};

try {
  const updateResult = db.settings.updateOne(
    { category: 'system' },
    { 
      $set: {
        'settings.weekly_workflow_settings': weeklyWorkflowSettings,
        'settings.weekly_configuration': {
          enabled: true,
          auto_schedule_enabled: true,
          timezone: 'Africa/Nairobi',
          global_limits: {
            max_weekly_links: 140,
            max_daily_links: 20,
            max_session_duration_hours: 2
          },
          notification_settings: {
            notify_on_schedule_start: false,
            notify_on_schedule_complete: false,
            notify_on_errors: true
          },
          created_at: new Date(),
          migration_version: '001_weekly_settings_schema'
        },
        updated_at: new Date()
      }
    },
    { upsert: true }
  );
  
  console.log('✅ Updated system settings with weekly configuration');
} catch (e) {
  console.log(`❌ Error updating system settings: ${e.message}`);
}

// ==============================================================================
// 4. CREATE WEEKLY EXECUTION TRACKING COLLECTION
// ==============================================================================

console.log('4. Creating weekly execution tracking collection...');

try {
  db.createCollection("weekly_execution_tracking");
  
  // Create indexes for tracking collection
  db.weekly_execution_tracking.createIndex({ "week_start_date": 1, "day_of_week": 1 });
  db.weekly_execution_tracking.createIndex({ "execution_date": -1 });
  db.weekly_execution_tracking.createIndex({ "day_of_week": 1, "session_type": 1 });
  db.weekly_execution_tracking.createIndex({ "status": 1 });
  
  console.log('✅ Created weekly_execution_tracking collection with indexes');
} catch (e) {
  console.log("weekly_execution_tracking collection may already exist:", e.message);
}

// ==============================================================================
// 5. CREATE VIEWS FOR WEEKLY ANALYTICS
// ==============================================================================

console.log('5. Creating views for weekly analytics...');

// Create view for weekly schedule overview
try {
  db.createView("weekly_schedule_overview", "weekly_workflow_schedules", [
    {
      $addFields: {
        total_daily_links: {
          $add: [
            { $cond: [{ $eq: ["$morning_schedule.enabled", true] }, "$morning_schedule.links_to_filter", 0] },
            { $cond: [{ $eq: ["$evening_schedule.enabled", true] }, "$evening_schedule.links_to_filter", 0] }
          ]
        },
        total_daily_time: {
          $add: [
            { $cond: [{ $eq: ["$morning_schedule.enabled", true] }, "$morning_schedule.time_limit_hours", 0] },
            { $cond: [{ $eq: ["$evening_schedule.enabled", true] }, "$evening_schedule.time_limit_hours", 0] }
          ]
        },
        active_sessions: {
          $size: {
            $filter: {
              input: [
                { $cond: [{ $eq: ["$morning_schedule.enabled", true] }, "morning", null] },
                { $cond: [{ $eq: ["$evening_schedule.enabled", true] }, "evening", null] }
              ],
              cond: { $ne: ["$$this", null] }
            }
          }
        }
      }
    },
    {
      $project: {
        day_of_week: 1,
        day_name: 1,
        is_enabled: 1,
        schedule_type: 1,
        total_daily_links: 1,
        total_daily_time: 1,
        active_sessions: 1,
        morning_enabled: "$morning_schedule.enabled",
        evening_enabled: "$evening_schedule.enabled",
        morning_time: "$morning_schedule.time",
        evening_time: "$evening_schedule.time"
      }
    }
  ]);
  console.log('✅ Created weekly_schedule_overview view');
} catch (e) {
  console.log("weekly_schedule_overview view already exists or creation failed:", e.message);
}

// Create view for weekly execution summary
try {
  db.createView("weekly_execution_summary", "weekly_execution_tracking", [
    {
      $group: {
        _id: {
          week_start: "$week_start_date",
          day: "$day_of_week"
        },
        total_executions: { $sum: 1 },
        successful_executions: { 
          $sum: { $cond: [{ $eq: ["$status", "completed"] }, 1, 0] } 
        },
        failed_executions: { 
          $sum: { $cond: [{ $eq: ["$status", "failed"] }, 1, 0] } 
        },
        total_links_processed: { $sum: "$links_processed" },
        total_execution_time: { $sum: "$execution_duration_minutes" },
        avg_execution_time: { $avg: "$execution_duration_minutes" },
        last_execution: { $max: "$execution_date" }
      }
    },
    {
      $addFields: {
        success_rate: {
          $cond: [
            { $gt: ["$total_executions", 0] },
            { $multiply: [{ $divide: ["$successful_executions", "$total_executions"] }, 100] },
            0
          ]
        }
      }
    },
    {
      $sort: { "_id.week_start": -1, "_id.day": 1 }
    }
  ]);
  console.log('✅ Created weekly_execution_summary view');
} catch (e) {
  console.log("weekly_execution_summary view already exists or creation failed:", e.message);
}

// ==============================================================================
// 6. CREATE UTILITY FUNCTIONS
// ==============================================================================

console.log('6. Creating utility functions...');

// Function to get current week's schedule
db.system.js.save({
  _id: "getCurrentWeekSchedule",
  value: function() {
    const currentDay = new Date().toLocaleDateString('en-US', { weekday: 'lowercase' });
    
    return db.weekly_workflow_schedules.find(
      { is_enabled: true },
      { 
        day_of_week: 1, 
        day_name: 1, 
        morning_schedule: 1, 
        evening_schedule: 1,
        daily_limits: 1,
        _id: 0 
      }
    ).sort({ 
      day_of_week: 1 
    }).toArray();
  }
});

// Function to get today's schedule
db.system.js.save({
  _id: "getTodaySchedule",
  value: function() {
    const daysOfWeek = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
    const today = daysOfWeek[new Date().getDay()];
    
    return db.weekly_workflow_schedules.findOne(
      { day_of_week: today },
      { _id: 0 }
    );
  }
});

// Function to validate weekly settings
db.system.js.save({
  _id: "validateWeeklySettings",
  value: function() {
    const issues = [];
    const schedules = db.weekly_workflow_schedules.find({}).toArray();
    
    if (schedules.length !== 7) {
      issues.push(`Expected 7 day schedules, found ${schedules.length}`);
    }
    
    const enabledDays = db.weekly_workflow_schedules.find({ is_enabled: true }).count();
    if (enabledDays === 0) {
      issues.push("No days are enabled for workflow execution");
    }
    
    // Check for scheduling conflicts
    schedules.forEach(schedule => {
      if (schedule.is_enabled) {
        const morningEnabled = schedule.morning_schedule.enabled;
        const eveningEnabled = schedule.evening_schedule.enabled;
        
        if (!morningEnabled && !eveningEnabled) {
          issues.push(`${schedule.day_name} is enabled but has no active sessions`);
        }
        
        if (morningEnabled && eveningEnabled) {
          const morningTime = schedule.morning_schedule.time;
          const eveningTime = schedule.evening_schedule.time;
          
          if (morningTime >= eveningTime) {
            issues.push(`${schedule.day_name} morning time (${morningTime}) is after evening time (${eveningTime})`);
          }
        }
      }
    });
    
    return {
      timestamp: new Date(),
      issues: issues,
      isValid: issues.length === 0,
      enabledDays: enabledDays,
      totalSchedules: schedules.length,
      summary: issues.length === 0 ? "All weekly settings are valid" : `Found ${issues.length} validation issues`
    };
  }
});

// Function to get weekly statistics
db.system.js.save({
  _id: "getWeeklyStatistics",
  value: function() {
    const schedules = db.weekly_workflow_schedules.find({}).toArray();
    const enabledSchedules = schedules.filter(s => s.is_enabled);
    
    let totalWeeklyLinks = 0;
    let totalWeeklyTime = 0;
    let totalSessions = 0;
    
    enabledSchedules.forEach(schedule => {
      const morningLinks = schedule.morning_schedule.enabled ? schedule.morning_schedule.links_to_filter : 0;
      const eveningLinks = schedule.evening_schedule.enabled ? schedule.evening_schedule.links_to_filter : 0;
      const morningTime = schedule.morning_schedule.enabled ? schedule.morning_schedule.time_limit_hours : 0;
      const eveningTime = schedule.evening_schedule.enabled ? schedule.evening_schedule.time_limit_hours : 0;
      
      totalWeeklyLinks += morningLinks + eveningLinks;
      totalWeeklyTime += morningTime + eveningTime;
      totalSessions += (schedule.morning_schedule.enabled ? 1 : 0) + (schedule.evening_schedule.enabled ? 1 : 0);
    });
    
    return {
      timestamp: new Date(),
      enabled_days: enabledSchedules.length,
      total_days: schedules.length,
      total_weekly_links: totalWeeklyLinks,
      total_weekly_time_hours: totalWeeklyTime,
      total_weekly_sessions: totalSessions,
      average_links_per_day: totalWeeklyLinks / Math.max(enabledSchedules.length, 1),
      average_time_per_day: totalWeeklyTime / Math.max(enabledSchedules.length, 1),
      weekday_coverage: enabledSchedules.filter(s => s.schedule_type === 'weekday').length / 5 * 100,
      weekend_coverage: enabledSchedules.filter(s => s.schedule_type === 'weekend').length / 2 * 100
    };
  }
});

console.log('✅ Created utility functions');

// ==============================================================================
// 7. INITIAL VALIDATION AND STATISTICS
// ==============================================================================

console.log('7. Running initial validation and statistics...');

try {
  const validation = db.eval("validateWeeklySettings()");
  console.log('Weekly Settings Validation:', JSON.stringify(validation, null, 2));
  
  const statistics = db.eval("getWeeklyStatistics()");
  console.log('Weekly Statistics:', JSON.stringify(statistics, null, 2));
  
  const todaySchedule = db.eval("getTodaySchedule()");
  if (todaySchedule) {
    console.log(`Today's Schedule (${todaySchedule.day_name}):`, JSON.stringify(todaySchedule, null, 2));
  }
} catch (e) {
  console.log("Initial validation completed with issues:", e.message);
}

// ==============================================================================
// 8. RECORD MIGRATION
// ==============================================================================

console.log('8. Recording migration...');

try {
  db.system_maintenance.insertOne({
    operation_type: "schema_migration",
    migration_name: "001_weekly_settings_schema",
    migration_version: "1.0.0",
    status: "completed",
    details: {
      collections_created: [
        "weekly_workflow_schedules",
        "weekly_execution_tracking"
      ],
      views_created: [
        "weekly_schedule_overview",
        "weekly_execution_summary"
      ],
      functions_created: [
        "getCurrentWeekSchedule",
        "getTodaySchedule", 
        "validateWeeklySettings",
        "getWeeklyStatistics"
      ],
      settings_updated: [
        "weekly_workflow_settings",
        "weekly_configuration"
      ]
    },
    created_at: new Date(),
    completed_at: new Date()
  });
  
  console.log('✅ Migration recorded successfully');
} catch (e) {
  console.log("Migration recording completed:", e.message);
}

console.log('=== Weekly Settings Schema Migration Complete ===');
console.log('');
console.log('Summary:');
console.log('- Created weekly_workflow_schedules collection with 7 default schedules');
console.log('- Created weekly_execution_tracking collection');
console.log('- Updated system settings with weekly configuration');
console.log('- Created analytics views and utility functions');
console.log('- Validated configuration and generated statistics');
console.log('');
console.log('Next steps:');
console.log('1. Update your application to use the new weekly settings collections');
console.log('2. Test the weekly scheduling functionality');
console.log('3. Monitor weekly_execution_tracking for performance data');
console.log('4. Use validateWeeklySettings() to check configuration health');
console.log('5. Use getWeeklyStatistics() for scheduling analytics');
console.log('');