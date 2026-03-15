import json
import os
from pathlib import Path


def update_workflow_url(input_file, output_file, old_url, new_url):
    """
    Load a workflow JSON, replace all instances of old_url with new_url, and save to a new file
    
    Args:
        input_file (str): Path to input JSON file (e.g., 'workflow/kimu.json')
        output_file (str): Path to output JSON file (e.g., 'workflow/facebook.json')
        old_url (str): URL to replace (e.g., 'www.kimu.com')
        new_url (str): New URL (e.g., 'www.facebook.com')
    
    Returns:
        dict: Result containing success status and details
    """
    try:
        # Verify input file exists
        if not os.path.exists(input_file):
            return {
                "success": False,
                "error": f"Input file not found: {input_file}"
            }
        
        # Load the JSON file
        print(f"Loading workflow from: {input_file}")
        with open(input_file, 'r', encoding='utf-8') as f:
            workflow_data = json.load(f)
        
        print(f"Workflow loaded: {workflow_data.get('name', 'Unnamed')}")
        
        # Track replacements
        urls_replaced = 0
        blocks_checked = 0
        
        # Update URLs in drawflow.nodes structure
        if 'drawflow' in workflow_data and 'nodes' in workflow_data['drawflow']:
            for node in workflow_data['drawflow']['nodes']:
                if 'data' in node and 'blocks' in node['data']:
                    for block in node['data']['blocks']:
                        blocks_checked += 1
                        
                        # Check if block has url field
                        if 'data' in block and 'url' in block['data']:
                            if block['data']['url'] == old_url:
                                block['data']['url'] = new_url
                                urls_replaced += 1
                                print(f"  ✓ Updated URL in block '{block.get('id', 'unknown')}' "
                                      f"(itemId: {block.get('itemId', 'N/A')})")
        
        # Also check flat blocks structure (alternative format)
        elif 'blocks' in workflow_data:
            for block in workflow_data['blocks']:
                blocks_checked += 1
                
                if 'data' in block and 'url' in block['data']:
                    if block['data']['url'] == old_url:
                        block['data']['url'] = new_url
                        urls_replaced += 1
                        print(f"  ✓ Updated URL in block '{block.get('id', 'unknown')}'")
        
        # Check if any replacements were made
        if urls_replaced == 0:
            print(f"⚠ Warning: No instances of '{old_url}' found in {blocks_checked} blocks")
            return {
                "success": False,
                "error": f"No instances of '{old_url}' found",
                "blocks_checked": blocks_checked
            }
        
        # Update workflow name to reflect the change
        if 'name' in workflow_data:
            original_name = workflow_data['name']
            workflow_data['name'] = workflow_data['name'].replace('kimu', 'facebook')
            print(f"Updated workflow name: '{original_name}' -> '{workflow_data['name']}'")
        
        # Update description if it contains the old URL
        if 'description' in workflow_data and old_url in workflow_data['description']:
            workflow_data['description'] = workflow_data['description'].replace(old_url, new_url)
            print(f"Updated workflow description")
        
        # Ensure output directory exists
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            print(f"Created directory: {output_dir}")
        
        # Save the updated workflow
        print(f"Saving updated workflow to: {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(workflow_data, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Successfully saved updated workflow")
        
        return {
            "success": True,
            "input_file": input_file,
            "output_file": output_file,
            "old_url": old_url,
            "new_url": new_url,
            "blocks_checked": blocks_checked,
            "urls_replaced": urls_replaced,
            "workflow_name": workflow_data.get('name', 'Unnamed')
        }
    
    except json.JSONDecodeError as e:
        return {
            "success": False,
            "error": f"Invalid JSON format: {e}"
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Error processing workflow: {e}"
        }


def main():
    """Main function to update the workflow"""
    
    # Define paths and URLs
    input_file = "workflow/kimu.json"
    output_file = "workflow/facebook.json"
    old_url = "www.kimu.com"
    new_url = "www.facebook.com"
    
    print("=" * 60)
    print("Automa Workflow URL Updater")
    print("=" * 60)
    print(f"Input:  {input_file}")
    print(f"Output: {output_file}")
    print(f"Change: {old_url} -> {new_url}")
    print("=" * 60)
    print()
    
    # Execute the update
    result = update_workflow_url(input_file, output_file, old_url, new_url)
    
    # Print results
    print()
    print("=" * 60)
    if result['success']:
        print("✓ SUCCESS!")
        print(f"  Workflow Name: {result['workflow_name']}")
        print(f"  Blocks Checked: {result['blocks_checked']}")
        print(f"  URLs Replaced: {result['urls_replaced']}")
        print(f"  Output File: {result['output_file']}")
    else:
        print("✗ FAILED!")
        print(f"  Error: {result['error']}")
    print("=" * 60)
    
    return result


if __name__ == "__main__":
    main()