import { promises as fs } from 'fs';
import path from 'path';

// Function to save workflow to file (no cron modifications)
export async function saveWorkflowToFile(workflow, workflowType, workflowId) {
    try {
        const baseDir = '/opt/airflow/workflows';
        const typeDir = path.join(baseDir, workflowType);
        
        // Create base workflows directory if it doesn't exist
        await fs.mkdir(baseDir, { recursive: true });
        
        // Create type-specific directory (messages, replies, retweets)
        await fs.mkdir(typeDir, { recursive: true });
        
        const filePath = path.join(typeDir, `${workflowId}.json`);
        await fs.writeFile(filePath, JSON.stringify(workflow, null, 2));
        
        console.log(`   - ✅ Saved ${workflowType} workflow ${workflowId} to ${filePath}`);
        console.log(`   - ℹ️ Workflow ready for manual execution`);
        
        return filePath;
    } catch (error) {
        console.error(`   - ❌ Error saving ${workflowType} workflow ${workflowId} to file:`, error.message);
        return null;
    }
}

// Function to update workflow structure (removing cron trigger logic)
export function prepareWorkflowForManualExecution(workflow) {
    // Ensure the workflow has a proper structure
    if (!workflow.drawflow) {
        workflow.drawflow = { nodes: [] };
    }
    
    if (!workflow.drawflow.nodes || workflow.drawflow.nodes.length === 0) {
        workflow.drawflow.nodes = [];
    }
    
    // Remove any cron triggers if they exist (since we're going manual)
    workflow.drawflow.nodes.forEach(node => {
        if (node.data && node.data.triggers) {
            // Filter out cron triggers
            node.data.triggers = node.data.triggers.filter(trigger => trigger.type !== 'cron-job');
        }
    });
    
    console.log('   - ✅ Workflow prepared for manual execution (cron triggers removed)');
    return workflow;
}

// Function to validate workflow structure
export function validateWorkflow(workflow) {
    if (!workflow) {
        console.warn('   - ⚠️ Workflow is null or undefined');
        return false;
    }
    
    if (!workflow._id && !workflow.id) {
        console.warn('   - ⚠️ Workflow missing ID');
        return false;
    }
    
    if (!workflow.drawflow || !workflow.drawflow.nodes) {
        console.warn('   - ⚠️ Workflow missing drawflow structure');
        return false;
    }
    
    console.log('   - ✅ Workflow structure validated');
    return true;
}