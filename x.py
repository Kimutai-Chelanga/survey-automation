@staticmethod
def _display_actual_content(workflow: Dict[str, Any]):
    """Display workflow metadata with actual content."""
    st.subheader("Workflow Metadata")
    
    # Create 4 columns instead of 3
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.write("**Content Information:**")
        st.write(f"• **Content ID:** {workflow.get('content_id', 'N/A')}")
        st.write(f"• **Content Type:** {workflow.get('content_type', 'N/A')}")
        st.write(f"• **Has Content:** {'✅ Yes' if workflow.get('has_content') else '❌ No'}")
        st.write(f"• **Has Link:** {'✅ Yes' if workflow.get('has_link') else '❌ No'}")
    
    with col2:
        st.write("**Account Information:**")
        st.write(f"• **Account ID:** {workflow.get('account_id', 'N/A')}")
        st.write(f"• **Username:** {workflow.get('username', 'N/A')}")
        st.write(f"• **Profile ID:** {workflow.get('profile_id', 'N/A')}")
    
    with col3:
        st.write("**Execution Status:**")
        st.write(f"• **Status:** {workflow.get('status', 'unknown')}")
        st.write(f"• **Executed:** {'✅ Yes' if workflow.get('executed') else '❌ No'}")
        st.write(f"• **Success:** {'✅ Yes' if workflow.get('success') else '❌ No'}")
        st.write(f"• **Blocks Generated:** {workflow.get('blocks_generated', 0)}")
    
    with col4:
        st.write("**Link Information:**")
        if workflow.get('link_url'):
            st.write(f"• **Link URL:** [{workflow.get('link_url')[:30]}...]({workflow.get('link_url')})")
        else:
            st.write(f"• **Link URL:** N/A")
        st.write(f"• **Linked At:** {workflow.get('link_assigned_at', 'N/A')}")
    
    # ADD ACTUAL CONTENT DISPLAY
    if workflow.get('actual_content'):
        st.markdown("---")
        st.subheader("📄 Actual Content")
        st.text_area(
            "Content Text:",
            value=workflow.get('actual_content'),
            height=200,
            disabled=True,
            key=f"actual_content_{workflow.get('_id')}"
        )
    
    # Show errors if any
    if workflow.get('error_message'):
        st.error(f"**Error Message:** {workflow.get('error_message')}")