#!/bin/bash

# Create the corrected orchestrator section
docker exec airflow_scheduler bash -c 'cat > /tmp/orchestrator-fix.txt << "ORCHEOF"
                try {
                    // Execute the workflow with correct method name and parameters
                    const result = await this.workflowExecutor.execute(
                        combo,           // linkData
                        combo,           // workflowMetadata (same object has both)
                        workflowNumber,  // workflowNumber
                        combinations.length  // totalWorkflows
                    );

                    // Update statistics based on result
                    if (result.success) {
                        this.stats.successfulWorkflows++;
                        console.log(`   ✅ SUCCESS - Execution completed`);

                        // Update PostgreSQL success flag
                        if (this.config.updatePostgresSuccess) {
                            const pgUpdate = await this.pgService.updateLinkSuccessStatus(
                                combo.links_id,
                                true  // success = true
                            );
                            if (pgUpdate) {
                                this.stats.postgresUpdates.success++;
                                console.log(`   📊 PostgreSQL: success=TRUE, failure=FALSE`);
                            }
                        }
                    } else {
                        this.stats.failedWorkflows++;
                        console.log(`   ❌ FAILURE - ${result.error || "Unknown error"}`);

                        // Update PostgreSQL failure flag
                        if (this.config.updatePostgresFailure) {
                            const pgUpdate = await this.pgService.updateLinkFailureStatus(
                                combo.links_id,
                                result.error || "Workflow execution failed"
                            );
                            if (pgUpdate) {
                                this.stats.postgresUpdates.failure++;
                                console.log(`   📊 PostgreSQL: success=FALSE, failure=TRUE`);
                            }
                        }
                    }
ORCHEOF
'

# Backup original file
docker exec airflow_scheduler cp /opt/airflow/src/scripts/local_execute/local-orchestrator.js /opt/airflow/src/scripts/local_execute/local-orchestrator.js.backup

# Apply the fix using sed
docker exec airflow_scheduler bash -c "sed -i 's/const result = await this\.workflowExecutor\.executeWorkflow(combo);/const result = await this.workflowExecutor.execute(combo, combo, workflowNumber, combinations.length);/g' /opt/airflow/src/scripts/local_execute/local-orchestrator.js"

echo "✅ Orchestrator method call fixed"

# Verify
echo "📝 Verifying fix..."
docker exec airflow_scheduler grep -n "this.workflowExecutor.execute" /opt/airflow/src/scripts/local_execute/local-orchestrator.js | head -1

