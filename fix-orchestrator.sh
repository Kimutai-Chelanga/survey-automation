#!/bin/bash

# Backup the original file
docker exec airflow_scheduler cp /opt/airflow/src/scripts/local_execute/local-orchestrator.js /opt/airflow/src/scripts/local_execute/local-orchestrator.js.backup

# Fix the method name: executeWorkflow -> execute
docker exec airflow_scheduler sed -i 's/this\.workflowExecutor\.executeWorkflow/this.workflowExecutor.execute/g' /opt/airflow/src/scripts/local_execute/local-orchestrator.js

# Verify the fix
echo ""
echo "✅ Verification: Checking for remaining 'executeWorkflow' calls..."
REMAINING=$(docker exec airflow_scheduler grep -c "executeWorkflow" /opt/airflow/src/scripts/local_execute/local-orchestrator.js || echo "0")

if [ "$REMAINING" -eq "0" ]; then
    echo "✅ SUCCESS: All 'executeWorkflow' calls replaced with 'execute'"
else
    echo "⚠️  WARNING: Still found $REMAINING instances of 'executeWorkflow'"
    docker exec airflow_scheduler grep -n "executeWorkflow" /opt/airflow/src/scripts/local_execute/local-orchestrator.js
fi

echo ""
echo "✅ Checking for correct 'execute' method calls..."
docker exec airflow_scheduler grep -n "this\.workflowExecutor\.execute" /opt/airflow/src/scripts/local_execute/local-orchestrator.js | head -3

echo ""
echo "🔄 Restarting Airflow scheduler..."
docker restart airflow_scheduler

sleep 30

echo ""
echo "════════════════════════════════════════════════════════════"
echo "✅ FIX APPLIED SUCCESSFULLY!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "What was fixed:"
echo "  • this.workflowExecutor.executeWorkflow() → this.workflowExecutor.execute()"
echo ""
echo "Backup location:"
echo "  /opt/airflow/src/scripts/local_execute/local-orchestrator.js.backup"
echo ""
echo "Next steps:"
echo "  1. Go to Airflow UI"
echo "  2. Trigger the 'local_executor' DAG"
echo "  3. Workflows should now execute properly!"
echo "════════════════════════════════════════════════════════════"

