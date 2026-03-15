import streamlit as st
from ...settings.settings_manager import (
    get_system_setting,
    update_system_setting,
    get_postgres_prompt_types,
    sync_postgres_prompt_types_to_settings
)
from datetime import datetime, timedelta, date
from typing import Dict, Any, List
import json
import os


def get_future_dates(num_dates: int = 8) -> list:
    dates = []
    today = datetime.now().date()
    for i in range(num_dates):
        future_date = today + timedelta(days=i)
        date_str = future_date.strftime('%b %d, %Y')
        if i == 0:
            date_str += " (Today)"
        dates.append((future_date, date_str))
    return dates


def get_day_key_from_date(target_date: date) -> str:
    weekday = target_date.weekday()
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    return days[weekday]


def check_automa_config_exists(weekly_settings: Dict, target_date: date) -> tuple:
    day_key    = get_day_key_from_date(target_date)
    day_config = weekly_settings.get(day_key, {})

    if not day_config or 'automa_workflow_config' not in day_config:
        return (False, day_key, {})

    automa_config   = day_config.get('automa_workflow_config', {})
    stored_date_str = automa_config.get('config_date')

    if not stored_date_str:
        return (False, day_key, {})

    try:
        stored_date = date.fromisoformat(stored_date_str)
        if stored_date == target_date:
            return (True, day_key, automa_config)
        else:
            return (False, day_key, {})
    except:
        return (False, day_key, {})


def get_all_prompt_types_from_db() -> List[str]:
    try:
        prompt_types = get_postgres_prompt_types()
        if prompt_types and len(prompt_types) > 0:
            return prompt_types
        st.warning("⚠️ No prompt types found in database.")
        return []
    except Exception as e:
        st.error(f"❌ Error fetching prompt types: {e}")
        return []


def get_default_automa_config():
    return {
        'press_keys': {
            'mode': 'range',
            'min_milliseconds': 0,
            'max_milliseconds': 10000
        },
        'click_elements': {
            'mode': 'range',
            'min_milliseconds': 0,
            'max_milliseconds': 10000
        },
        'typing_delay': {
            'mode': 'fixed',
            'min_milliseconds': 80,
            'max_milliseconds': 80
        }
    }


def get_template_categories() -> List[str]:
    try:
        from src.core.database.mongodb.connection import get_mongo_collection
        templates_collection = get_mongo_collection("workflow_templates")
        if templates_collection is not None:
            categories = templates_collection.distinct("category", {"is_active": True})
            categories = [cat.title() for cat in categories if cat and isinstance(cat, str)]
            categories.sort()
            return categories
        return []
    except Exception as e:
        st.error(f"❌ Error loading categories: {e}")
        return []


def check_collection_exists(collection_name: str) -> bool:
    try:
        from pymongo import MongoClient
        client      = MongoClient(
            os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            serverSelectionTimeoutMS=5000
        )
        db          = client['messages_db']
        collections = db.list_collection_names()
        client.close()
        return collection_name in collections
    except Exception as e:
        st.session_state.setdefault('debug_messages', []).append(f"Debug: Error checking collection - {str(e)}")
        return False


def get_saved_workflow_categories() -> List[str]:
    try:
        workflow_categories = get_system_setting('workflow_categories', {})
        if workflow_categories is None:
            update_system_setting('workflow_categories', {})
            return []
        if not isinstance(workflow_categories, dict):
            workflow_categories = {}
            update_system_setting('workflow_categories', workflow_categories)
            return []
        return sorted(list(workflow_categories.keys()))
    except RuntimeError as e:
        if "No default available for unknown setting key" in str(e):
            update_system_setting('workflow_categories', {})
            return []
        else:
            st.error(f"❌ Error loading saved workflow categories: {e}")
            return []
    except Exception as e:
        st.session_state.setdefault('debug_messages', []).append(f"Debug: Error loading categories - {str(e)}")
        update_system_setting('workflow_categories', {})
        return []


def get_workflow_types_for_category(category: str) -> List[str]:
    try:
        workflow_categories = get_system_setting('workflow_categories', {})
        if not isinstance(workflow_categories, dict):
            return []
        return workflow_categories.get(category, [])
    except Exception as e:
        return []


def save_workflow_category_and_type(category: str, workflow_type: str):
    try:
        workflow_categories = get_system_setting('workflow_categories', {})
        if not isinstance(workflow_categories, dict):
            workflow_categories = {}
        if category not in workflow_categories:
            workflow_categories[category] = []
        if workflow_type and workflow_type not in workflow_categories[category]:
            workflow_categories[category].append(workflow_type)
            workflow_categories[category].sort()
        update_system_setting('workflow_categories', workflow_categories)
        return True
    except Exception as e:
        st.error(f"❌ Error saving category/type: {e}")
        return False


def get_emoji_for_type(content_type: str) -> str:
    emoji_patterns = {
        'message': '💬', 'reply': '📝', 'retweet': '🔄',
        'tweet': '🐦', 'post': '📮', 'comment': '💭',
        'article': '📰', 'blog': '✍️', 'testimonial': '⭐',
        'review': '📊', 'feedback': '💡', 'update': '🔔',
        'announcement': '📢',
    }
    content_lower = content_type.lower()
    for pattern, emoji in emoji_patterns.items():
        if pattern in content_lower:
            return emoji
    return '📄'


# ============================================================================
# TIMING WIDGET HELPER — lives OUTSIDE the form so radio triggers rerender
# Only used for Press Keys and Click Elements (supports Range + Fixed).
# Typing Delay uses a dedicated fixed-only widget (render_typing_delay_config).
# ============================================================================

def render_timing_config(
    label: str,
    icon: str,
    existing_cfg: Dict[str, Any],
    key_prefix: str
) -> Dict[str, Any]:

    st.markdown(f"### {icon} {label} Configuration")

    saved_mode = existing_cfg.get("mode", "range")
    if saved_mode not in ("range", "fixed"):
        saved_mode = "range"

    mode_key = f"{key_prefix}_mode"

    if mode_key not in st.session_state:
        st.session_state[mode_key] = "Range" if saved_mode == "range" else "Fixed"

    mode = st.radio(
        f"Select {label} timing mode:",
        options=["Range", "Fixed"],
        horizontal=True,
        key=mode_key,
        help=(
            "Range — randomises between Min and Max each time.\n"
            "Fixed — uses exactly one value every time."
        )
    )

    current_mode = mode.lower()
    result: Dict[str, Any] = {"mode": current_mode}

    if current_mode == "range":
        st.session_state.pop(f"{key_prefix}_fixed", None)

        if f"{key_prefix}_min" not in st.session_state:
            st.session_state[f"{key_prefix}_min"] = int(
                existing_cfg.get("min_milliseconds", 0) if saved_mode == "range" else 0
            )
        if f"{key_prefix}_max" not in st.session_state:
            st.session_state[f"{key_prefix}_max"] = int(
                existing_cfg.get("max_milliseconds", 10000) if saved_mode == "range" else 10000
            )

        c1, c2 = st.columns(2)
        with c1:
            val_min = st.number_input(
                "Minimum (ms)",
                min_value=0, max_value=60000, step=1000,
                key=f"{key_prefix}_min"
            )
        with c2:
            val_max = st.number_input(
                "Maximum (ms)",
                min_value=0, max_value=60000, step=1000,
                key=f"{key_prefix}_max"
            )

        if val_min > val_max:
            st.error(f"⚠️ Minimum cannot exceed Maximum for {label}")

        st.caption(f"💡 Will randomise between **{val_min/1000:.2f}s** and **{val_max/1000:.2f}s**")

        result["min_milliseconds"] = val_min
        result["max_milliseconds"] = val_max

    else:
        st.session_state.pop(f"{key_prefix}_min", None)
        st.session_state.pop(f"{key_prefix}_max", None)

        if f"{key_prefix}_fixed" not in st.session_state:
            st.session_state[f"{key_prefix}_fixed"] = int(
                existing_cfg.get("max_milliseconds", 1000) if saved_mode == "fixed" else 1000
            )

        val_fixed = st.number_input(
            "Fixed Value (ms)",
            min_value=0, max_value=60000, step=1000,
            key=f"{key_prefix}_fixed",
            help="Will wait exactly this long every time."
        )

        st.caption(f"💡 Will always wait exactly **{val_fixed/1000:.2f}s**")

        result["min_milliseconds"] = val_fixed
        result["max_milliseconds"] = val_fixed

    return result


# ============================================================================
# TYPING DELAY WIDGET — fixed-only, maps directly to Automa's
# "Typing delay (millisecond)" field in the forms (text-field) node.
# ============================================================================

def render_typing_delay_config(existing_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Render a fixed-only typing delay input.

    This value is injected directly into the Automa forms node as:
        node['data']['delay'] = str(typing_delay_ms)

    It corresponds to Automa's built-in "Typing delay (millisecond)" field.
    Setting it to 0 disables per-character typing delay entirely.
    Range mode is intentionally NOT supported here — the value is a direct
    passthrough to Automa, not a workflow-level randomisation parameter.
    """
    st.markdown("### ⌚ Typing Delay Configuration")
    st.caption(
        "Sets the **Automa 'Typing delay (millisecond)'** field on the forms node. "
        "This controls the delay between each character typed into the text field. "
        "Set to **0** to disable. E.g. `4000` = 4 seconds between characters."
    )

    saved_val = int(existing_cfg.get('max_milliseconds', 80))

    if 'typing_delay_fixed_val' not in st.session_state:
        st.session_state['typing_delay_fixed_val'] = saved_val

    typing_fixed_val = st.number_input(
        "Typing Delay (ms)",
        min_value=0,
        max_value=60000,
        step=1000,
        key="typing_delay_fixed_val",
        help=(
            "Automa 'Typing delay (millisecond)' — applied to the forms / text-field node only. "
            "0 disables it. 4000 = 4 second delay between each character."
        )
    )

    st.caption(
        f"💡 Will set Automa typing delay to exactly **{typing_fixed_val}ms** "
        f"(**{typing_fixed_val/1000:.2f}s**)"
    )

    return {
        'mode': 'fixed',
        'min_milliseconds': typing_fixed_val,
        'max_milliseconds': typing_fixed_val,
    }


# ============================================================================
# AUTO-LOAD DEFAULT TEMPLATE FROM FILE
# ============================================================================

def load_default_template_from_file() -> Dict[str, Any]:
    candidate_paths = [
        os.path.join(os.getcwd(), 'src', 'templates', 'automa_template.automa.json'),
        os.path.join(os.getcwd(), 'templates', 'automa_template.automa.json'),
        '/opt/airflow/src/templates/automa_template.automa.json',
        '/app/src/templates/automa_template.automa.json',
        os.path.join(os.path.dirname(__file__), '..', '..', '..', 'templates', 'automa_template.automa.json'),
    ]

    for path in candidate_paths:
        normalized = os.path.normpath(path)
        if os.path.isfile(normalized):
            try:
                with open(normalized, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return data
            except Exception as e:
                st.warning(f"⚠️ Found template file at {normalized} but failed to parse: {e}")
                return None

    return None


def ensure_default_template_in_mongo() -> tuple:
    DEFAULT_CATEGORY = 'x'
    DEFAULT_NAME     = 'Automation_template'

    try:
        from src.core.database.mongodb.connection import get_mongo_collection
        from bson import ObjectId

        col = get_mongo_collection("workflow_templates")
        if col is None:
            return None, None, False

        existing = col.find_one(
            {'template_name': DEFAULT_NAME, 'category': DEFAULT_CATEGORY, 'is_active': True}
        )
        if existing:
            return str(existing['_id']), DEFAULT_NAME, True

        workflow_data = load_default_template_from_file()
        if not workflow_data:
            return None, None, False

        doc = {
            'template_name':          DEFAULT_NAME,
            'category':               DEFAULT_CATEGORY,
            'description':            'Default Automa workflow template (auto-loaded from file)',
            'tags':                   ['default', 'automa', 'x'],
            'workflow_data':          workflow_data,
            'created_at':             datetime.now().isoformat(),
            'updated_at':             datetime.now().isoformat(),
            'version':                workflow_data.get('version', '1.0'),
            'original_workflow_name': workflow_data.get('name', DEFAULT_NAME),
            'block_count':            _count_blocks(workflow_data),
            'is_active':              True,
        }
        result = col.insert_one(doc)
        return str(result.inserted_id), DEFAULT_NAME, False

    except Exception as e:
        st.warning(f"⚠️ Could not auto-load default template: {e}")
        return None, None, False


def _count_blocks(workflow_data: Dict[str, Any]) -> int:
    drawflow = workflow_data.get('drawflow', {})
    nodes = drawflow.get('nodes', {})
    if isinstance(nodes, (dict, list)):
        return len(nodes)
    return 0


def get_prompts_by_type_for_automa(content_type: str) -> List[Dict[str, Any]]:
    try:
        from src.core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT p.prompt_id, p.name AS prompt_name, p.account_id, a.username
                    FROM prompts p
                    LEFT JOIN accounts a ON p.account_id = a.account_id
                    WHERE p.prompt_type = %s AND p.is_active = TRUE
                    ORDER BY a.username, p.name
                """, (content_type,))
                return [dict(r) for r in cursor.fetchall()]
    except Exception:
        return []


# ============================================================================
# MAIN RENDER FUNCTION
# ============================================================================

def render_create_automa_workflow_config():
    try:
        get_system_setting('workflow_categories')
    except RuntimeError as e:
        if "No default available for unknown setting key" in str(e):
            update_system_setting('workflow_categories', {})

    st.header("🤖 Create Automa Workflow Configuration")
    st.markdown("*Configure workflow timing, interaction settings, and content selection*")

    DEFAULT_CATEGORY = 'x'
    DEFAULT_NAME     = 'Automation_template'

    if 'default_template_checked' not in st.session_state:
        with st.spinner("🔄 Loading default template..."):
            tid, tname, existed = ensure_default_template_in_mongo()
        if tid:
            st.session_state['default_template_id']      = tid
            st.session_state['default_template_name']    = tname
            st.session_state['default_template_existed'] = existed
            if not existed:
                st.success("✅ Default template auto-loaded from file into MongoDB")
            save_workflow_category_and_type(DEFAULT_CATEGORY, DEFAULT_CATEGORY)
        else:
            st.warning("⚠️ Default template file not found at src/templates/automa_template.automa.json — you can still select manually")
            st.session_state['default_template_id']   = None
            st.session_state['default_template_name'] = None
        st.session_state['default_template_checked'] = True

    default_template_id   = st.session_state.get('default_template_id')
    default_template_name = st.session_state.get('default_template_name', DEFAULT_NAME)

    if 'automa_form_initialized' not in st.session_state:
        st.session_state.automa_form_initialized = True

    try:
        available_prompt_types = get_all_prompt_types_from_db()

        if not available_prompt_types:
            st.error("❌ No prompt types available. Please create prompts first.")
            st.info("💡 Go to **Prompts** page to create custom prompt types")
            return

        st.success(f"✅ **{len(available_prompt_types)} content type(s) available**")
        type_display = ", ".join([f"`{pt}`" for pt in available_prompt_types])
        st.markdown(f"**Available types:** {type_display}")

        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("🔄 Refresh Prompt Types"):
                sync_postgres_prompt_types_to_settings()
                if 'default_template_checked' in st.session_state:
                    del st.session_state['default_template_checked']
                st.rerun()

        def get_all_active_prompts_flat() -> List[Dict[str, Any]]:
            all_p = []
            for ct in available_prompt_types:
                for p in get_prompts_by_type_for_automa(ct):
                    p['_content_type'] = ct
                    all_p.append(p)
            return all_p

        all_prompts_flat = get_all_active_prompts_flat()

        def slot_default(slot_idx: int) -> Dict[str, Any]:
            if not all_prompts_flat:
                return {
                    'content_type': available_prompt_types[0],
                    'content_name': ''
                }
            p = all_prompts_flat[slot_idx % len(all_prompts_flat)]
            return {
                'content_type': p['_content_type'],
                'content_name': p['prompt_name'],
            }

        weekly_settings = get_system_setting('weekly_workflow_settings', {})

        st.markdown("#### 📅 Schedule Date")
        available_dates = get_future_dates(8)
        date_options    = [date_str for _, date_str in available_dates]
        date_values     = [d        for d, _        in available_dates]

        selected_date_str = st.selectbox(
            "Schedule Date", options=date_options, index=0,
            help="Select the date for automa workflow configuration (showing next 8 days)",
            key="schedule_date_select"
        )
        selected_index = date_options.index(selected_date_str)
        scheduled_date = date_values[selected_index]

        config_exists, day_key, existing_config = check_automa_config_exists(weekly_settings, scheduled_date)
        day_name = scheduled_date.strftime('%A')

        if config_exists:
            st.success(f"✅ Automa configuration exists for {day_name}, {scheduled_date.strftime('%B %d, %Y')}")
        else:
            st.info(f"ℹ️ No automa configuration found for {day_name}, {scheduled_date.strftime('%B %d, %Y')}")

        if not existing_config:
            existing_config = get_default_automa_config()

        st.markdown("---")

        # ====================================================================
        # TIMING CONFIG — Press Keys and Click Elements (Range + Fixed)
        # ====================================================================

        press_keys_cfg = render_timing_config(
            label="Press Keys",
            icon="⌨️",
            existing_cfg=existing_config.get('press_keys', {}),
            key_prefix="press_keys"
        )

        st.markdown("---")

        click_elements_cfg = render_timing_config(
            label="Click Elements",
            icon="🖱️",
            existing_cfg=existing_config.get('click_elements', {}),
            key_prefix="click_elements"
        )

        st.markdown("---")

        # ====================================================================
        # TYPING DELAY — Fixed only (maps directly to Automa forms node delay)
        # ====================================================================

        typing_delay_cfg = render_typing_delay_config(
            existing_cfg=existing_config.get('typing_delay', {})
        )

        st.markdown("---")

        # ====================================================================
        # MAIN FORM
        # ====================================================================
        with st.form("automa_workflow_form", clear_on_submit=False):

            st.markdown("### 📝 Content Items (3 slots)")
            st.caption("CONTENT1, CONTENT2, CONTENT3 — each uses a different prompt by default")

            existing_content_items = existing_config.get('content_items', [])
            if not existing_content_items and 'content_type' in existing_config:
                existing_content_items = [{
                    'label': 'CONTENT1',
                    'content_type': existing_config.get('content_type', available_prompt_types[0]),
                    'content_name': existing_config.get('content_name', '')
                }]

            content_items = []

            for i in range(3):
                content_label = f"CONTENT{i+1}"
                saved_item    = existing_content_items[i] if i < len(existing_content_items) else {}
                default       = slot_default(i)

                st.markdown(f"""
                    <div style='background-color: rgba(28, 131, 225, 0.15);
                                padding: 15px; border-radius: 8px; margin: 15px 0;
                                border-left: 4px solid #1c83e1;'>
                        <h4 style='margin: 0; color: #1c83e1;'>📄 {content_label}</h4>
                    </div>
                """, unsafe_allow_html=True)

                # CONTENT3 note
                if i == 2:
                    st.info(
                        "📋 **CONTENT3** is injected into the Automa forms (text-field) node "
                        "using the **Typing Delay** configured above."
                    )

                col1, col2 = st.columns(2)

                with col1:
                    current_content_type = (
                        saved_item.get('content_type', default['content_type'])
                        if saved_item else default['content_type']
                    )
                    if current_content_type not in available_prompt_types:
                        current_content_type = available_prompt_types[0]

                    try:
                        ct_index = available_prompt_types.index(current_content_type)
                    except ValueError:
                        ct_index = 0

                    def format_ct(ct):
                        return f"{get_emoji_for_type(ct)} {ct.capitalize()}"

                    content_type = st.selectbox(
                        "Content Type",
                        options=available_prompt_types,
                        format_func=format_ct,
                        index=ct_index,
                        key=f"content_type_select_{i}"
                    )

                with col2:
                    default_cn = saved_item.get('content_name', default['content_name']) if saved_item else default['content_name']

                    try:
                        from src.core.database.postgres.content_handler import get_content_handler
                        handler       = get_content_handler(content_type)
                        content_names = handler.get_all_content_names()

                        if content_names:
                            default_index = 0
                            if default_cn and default_cn in content_names:
                                default_index = content_names.index(default_cn)

                            content_name = st.selectbox(
                                "Content Name",
                                options=content_names,
                                index=default_index,
                                key=f"content_name_select_{i}"
                            )
                            st.info(f"📋 Selected: {content_name}")
                        else:
                            content_name = st.text_input(
                                "Content Name",
                                value=default_cn,
                                key=f"content_name_input_{i}"
                            )
                    except Exception as e:
                        content_name = st.text_input(
                            "Content Name",
                            value=default_cn,
                            key=f"content_name_fallback_{i}"
                        )

                content_items.append({
                    'label':        content_label,
                    'content_type': content_type,
                    'content_name': content_name
                })

                if i < 2:
                    st.markdown("---")

            st.markdown("---")

            # ── Workflow Source ───────────────────────────────────────────────
            st.markdown("### 📂 Workflow Source (Template)")

            template_categories = get_template_categories()

            if 'X' not in template_categories and default_template_id:
                template_categories = ['X'] + template_categories

            col1, col2 = st.columns(2)

            with col1:
                saved_source_cat = existing_config.get('source_category', DEFAULT_CATEGORY)
                cat_titles = [c.title() for c in template_categories]

                if 'X' in cat_titles:
                    cat_index = cat_titles.index('X')
                elif saved_source_cat.title() in cat_titles:
                    cat_index = cat_titles.index(saved_source_cat.title())
                else:
                    cat_index = 0

                category_options = template_categories + ["➕ Create New Category"]

                selected_source_category = st.selectbox(
                    "Source Category:",
                    options=category_options,
                    index=cat_index,
                    key="source_category_select"
                )
                source_category = (
                    st.text_input("New Source Category Name:", key="new_source_category_input",
                                  placeholder="e.g., Social Media")
                    if selected_source_category == "➕ Create New Category"
                    else selected_source_category
                )

            with col2:
                workflow_source_id   = ''
                workflow_source_name = ''
                selected_template    = {}

                if source_category and source_category != "➕ Create New Category":
                    try:
                        from src.core.database.mongodb.connection import get_mongo_collection

                        cache_key = 'templates_cache'
                        if cache_key in st.session_state:
                            all_templates = st.session_state[cache_key]
                        else:
                            tc = get_mongo_collection("workflow_templates")
                            all_templates = list(tc.find(
                                {"is_active": True},
                                {"_id": 1, "template_name": 1, "category": 1,
                                 "description": 1, "created_at": 1, "version": 1}
                            ).sort("template_name", 1)) if tc is not None else []
                            st.session_state[cache_key] = all_templates

                        templates = [
                            t for t in all_templates
                            if t.get('category', '').lower() == source_category.lower()
                        ]

                        if templates:
                            template_names = [t['template_name'] for t in templates]
                            template_map   = {t['template_name']: t for t in templates}

                            name_index = 0
                            if DEFAULT_NAME in template_names:
                                name_index = template_names.index(DEFAULT_NAME)
                            else:
                                saved_wf_name = existing_config.get('workflow_source_name', DEFAULT_NAME)
                                if saved_wf_name in template_names:
                                    name_index = template_names.index(saved_wf_name)

                            selected_template_name = st.selectbox(
                                "Template Name:",
                                options=template_names,
                                index=name_index,
                                key="source_template_select"
                            )

                            selected_template    = template_map[selected_template_name]
                            workflow_source_id   = str(selected_template['_id'])
                            workflow_source_name = selected_template['template_name']
                            st.success(f"✅ Selected: {workflow_source_name}")
                        else:
                            if default_template_id and source_category.lower() == DEFAULT_CATEGORY:
                                workflow_source_id   = default_template_id
                                workflow_source_name = DEFAULT_NAME
                                st.success(f"✅ Auto-selected: {DEFAULT_NAME}")
                                st.session_state.pop('templates_cache', None)
                            else:
                                st.warning(f"⚠️ No templates found in '{source_category}' category")
                    except Exception as e:
                        st.error(f"❌ Error loading templates: {e}")
                else:
                    st.info("👆 Select or create a category first")

            if workflow_source_id:
                with st.expander("📋 Template Details"):
                    c1, c2 = st.columns(2)
                    with c1:
                        st.write(f"**Category:** {source_category}")
                        st.write(f"**Version:** {selected_template.get('version', '1.0') if selected_template else '1.0'}")
                    with c2:
                        st.write(f"**Template ID:** `{workflow_source_id}`")

            st.markdown("---")

            # ── Workflow Destination ──────────────────────────────────────────
            st.markdown("### 💾 Workflow Destination")

            saved_categories = get_saved_workflow_categories()
            if DEFAULT_CATEGORY not in [c.lower() for c in saved_categories]:
                save_workflow_category_and_type(DEFAULT_CATEGORY, DEFAULT_CATEGORY)
                saved_categories = get_saved_workflow_categories()

            col1, col2 = st.columns(2)

            with col1:
                current_dest_cat = existing_config.get('destination_category', DEFAULT_CATEGORY)
                dest_cat_options = saved_categories + ["➕ Create New Category"]

                dest_cat_index = 0
                for idx, sc in enumerate(saved_categories):
                    if sc.lower() == current_dest_cat.lower():
                        dest_cat_index = idx
                        break

                selected_dest_category = st.selectbox(
                    "Workflow Category:",
                    options=dest_cat_options,
                    index=dest_cat_index,
                    key="dest_category_select"
                )
                destination_category = (
                    st.text_input("New Category Name:", key="new_dest_category_input",
                                  placeholder="e.g., Social Media")
                    if selected_dest_category == "➕ Create New Category"
                    else selected_dest_category
                )

            with col2:
                workflow_type = ''
                if destination_category and destination_category.strip():
                    existing_types = get_workflow_types_for_category(destination_category)

                    if DEFAULT_CATEGORY not in existing_types and destination_category.lower() == DEFAULT_CATEGORY:
                        save_workflow_category_and_type(DEFAULT_CATEGORY, DEFAULT_CATEGORY)
                        existing_types = get_workflow_types_for_category(destination_category)

                    if existing_types:
                        type_options = existing_types + ["➕ Create New Type"]
                        current_wt   = existing_config.get('workflow_type', DEFAULT_CATEGORY)
                        type_index   = 0
                        if current_wt in existing_types:
                            type_index = existing_types.index(current_wt)
                        elif DEFAULT_CATEGORY in existing_types:
                            type_index = existing_types.index(DEFAULT_CATEGORY)

                        selected_type = st.selectbox(
                            "Workflow Type:",
                            options=type_options,
                            index=type_index,
                            key="workflow_type_select"
                        )
                        workflow_type = (
                            st.text_input("New Workflow Type:", key="new_workflow_type_input",
                                          placeholder="e.g., scheduled_posts")
                            if selected_type == "➕ Create New Type"
                            else selected_type
                        )
                    else:
                        workflow_type = st.text_input(
                            "Create First Workflow Type:",
                            value=DEFAULT_CATEGORY,
                            key="first_workflow_type_input"
                        )
                else:
                    st.info("👆 Select or create a category first")

            if destination_category and destination_category.strip() and workflow_type and workflow_type.strip():
                st.success(f"✅ Category: **{destination_category}** | Type: **{workflow_type}**")

            st.markdown("---")

            # ── Collection name ───────────────────────────────────────────────
            st.markdown("### 📁 Collection Configuration")

            collection_name = ''
            if destination_category and destination_category.strip() and workflow_type and workflow_type.strip():
                collection_name = f"{destination_category.lower()}_{workflow_type.lower()}"
                if workflow_source_name:
                    collection_name += f"_{workflow_source_name.lower().replace(' ', '_').replace('-', '_')}"
                collection_name += f"_{scheduled_date.strftime('%Y%m%d')}"
                collection_name  = ''.join(c for c in collection_name if c.isalnum() or c in ['_', '-'])

                st.info(f"**Collection Name:** `{collection_name}`")
                if check_collection_exists(collection_name):
                    st.warning(f"⚠️ Collection '{collection_name}' already exists")
                else:
                    st.success(f"✅ Collection '{collection_name}' will be created")
            else:
                st.info("👆 Complete category and type selection to see collection name")

            st.markdown("---")

            # ── Timing summary ────────────────────────────────────────────────
            st.markdown("### ⏱️ Timing Summary")
            ts1, ts2, ts3 = st.columns(3)
            with ts1:
                if press_keys_cfg['mode'] == 'fixed':
                    st.info(f"⌨️ Press Keys: Fixed **{press_keys_cfg['max_milliseconds']/1000:.2f}s**")
                else:
                    st.info(f"⌨️ Press Keys: **{press_keys_cfg['min_milliseconds']/1000:.2f}s** – **{press_keys_cfg['max_milliseconds']/1000:.2f}s**")
            with ts2:
                if click_elements_cfg['mode'] == 'fixed':
                    st.info(f"🖱️ Click Elements: Fixed **{click_elements_cfg['max_milliseconds']/1000:.2f}s**")
                else:
                    st.info(f"🖱️ Click Elements: **{click_elements_cfg['min_milliseconds']/1000:.2f}s** – **{click_elements_cfg['max_milliseconds']/1000:.2f}s**")
            with ts3:
                # Typing delay is always fixed — direct passthrough to Automa forms node
                td_ms = typing_delay_cfg['max_milliseconds']
                st.info(f"⌚ Typing Delay: Fixed **{td_ms}ms** ({td_ms/1000:.2f}s) — Automa forms node")

            st.markdown("---")

            # ── Summary ───────────────────────────────────────────────────────
            st.markdown("### 📊 Configuration Summary")
            sc1, sc2 = st.columns(2)
            with sc1:
                st.markdown("**📝 Content Items**")
                for item in content_items:
                    emoji = get_emoji_for_type(item['content_type'])
                    st.write(f"• {item['label']}: {emoji} {item['content_type'].capitalize()} - {item['content_name']}")
                st.write(f"• Source: {source_category} → {workflow_source_name or 'Not selected'}")
            with sc2:
                st.markdown("**💾 Destination & Timing**")
                st.write(f"• Category: {destination_category or 'Not specified'}")
                st.write(f"• Workflow Type: {workflow_type or 'Not specified'}")
                st.write(f"• Collection: {collection_name or 'Not specified'}")
                if press_keys_cfg['mode'] == 'fixed':
                    st.write(f"• Press: Fixed {press_keys_cfg['max_milliseconds']/1000:.2f}s")
                else:
                    st.write(f"• Press: {press_keys_cfg['min_milliseconds']/1000:.2f}s – {press_keys_cfg['max_milliseconds']/1000:.2f}s")
                if click_elements_cfg['mode'] == 'fixed':
                    st.write(f"• Click: Fixed {click_elements_cfg['max_milliseconds']/1000:.2f}s")
                else:
                    st.write(f"• Click: {click_elements_cfg['min_milliseconds']/1000:.2f}s – {click_elements_cfg['max_milliseconds']/1000:.2f}s")
                # Typing delay always fixed
                st.write(f"• Typing Delay: Fixed {typing_delay_cfg['max_milliseconds']}ms (Automa forms node)")

            # ── Validation ────────────────────────────────────────────────────
            validation_messages = []
            if not all(item['content_name'] for item in content_items):
                validation_messages.append("⚠️ Please specify content names for all 3 items")
            if not workflow_source_id:
                validation_messages.append("⚠️ Please select a workflow template")
            if not destination_category or not destination_category.strip():
                validation_messages.append("⚠️ Please specify destination category")
            if not workflow_type or not workflow_type.strip():
                validation_messages.append("⚠️ Please specify workflow type")
            if not collection_name:
                validation_messages.append("⚠️ Collection name not generated")
            if press_keys_cfg['mode'] == 'range' and press_keys_cfg['min_milliseconds'] > press_keys_cfg['max_milliseconds']:
                validation_messages.append("⚠️ Press time min > max")
            if click_elements_cfg['mode'] == 'range' and click_elements_cfg['min_milliseconds'] > click_elements_cfg['max_milliseconds']:
                validation_messages.append("⚠️ Click time min > max")
            # No range validation needed for typing_delay — always fixed

            has_errors = bool(validation_messages)
            for msg in validation_messages:
                st.warning(msg)

            st.markdown("---")

            # ── Save scope ────────────────────────────────────────────────────
            st.markdown("### 💾 Save Options")

            ALL_DAYS        = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            ALL_DAYS_LABELS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            WEEKDAYS        = ALL_DAYS[:5]
            WEEKDAYS_LABELS = ALL_DAYS_LABELS[:5]

            save_scope = st.radio(
                "Apply configuration to:",
                options=["Single day (selected above)", "Full week (Mon – Sun)", "Weekdays only (Mon – Fri)", "Custom days"],
                index=0,
                horizontal=True,
                key="save_scope_radio"
            )

            custom_days_selected: List[str] = []
            if save_scope == "Custom days":
                st.markdown("**Select days:**")
                day_cols = st.columns(7)
                for col_idx, (dk_opt, dk_label) in enumerate(zip(ALL_DAYS, ALL_DAYS_LABELS)):
                    with day_cols[col_idx]:
                        checked = st.checkbox(dk_label[:3], key=f"custom_day_{dk_opt}", value=True)
                        if checked:
                            custom_days_selected.append(dk_opt)

                if not custom_days_selected:
                    st.warning("⚠️ Please select at least one day")

            def _target_days() -> List[tuple]:
                today_d = datetime.now().date()
                day_to_next_date: Dict[str, date] = {}
                for offset in range(7):
                    d  = today_d + timedelta(days=offset)
                    dk = ALL_DAYS[d.weekday()]
                    if dk not in day_to_next_date:
                        day_to_next_date[dk] = d

                if save_scope == "Single day (selected above)":
                    keys = [day_key]
                elif save_scope == "Full week (Mon – Sun)":
                    keys = ALL_DAYS
                elif save_scope == "Weekdays only (Mon – Fri)":
                    keys = WEEKDAYS
                else:
                    keys = custom_days_selected

                result = []
                for dk in keys:
                    label_str = ALL_DAYS_LABELS[ALL_DAYS.index(dk)]
                    target_d  = day_to_next_date.get(dk, today_d)
                    result.append((dk, label_str, target_d))
                return result

            target_days = _target_days()

            if save_scope != "Single day (selected above)" and target_days:
                labels_preview = ", ".join([f"**{dl}**" for _, dl, _ in target_days])
                st.info(f"📅 Will save to: {labels_preview}")

            save_disabled = has_errors or (save_scope == "Custom days" and not custom_days_selected)

            if save_scope == "Single day (selected above)":
                btn_label = f"💾 Save for {day_name}"
            elif save_scope == "Full week (Mon – Sun)":
                btn_label = "📅 Save for Full Week (Mon – Sun)"
            elif save_scope == "Weekdays only (Mon – Fri)":
                btn_label = "📅 Save for Weekdays (Mon – Fri)"
            else:
                n = len(custom_days_selected)
                btn_label = f"📅 Save for {n} Selected Day{'s' if n != 1 else ''}"

            submit_button = st.form_submit_button(
                btn_label,
                type="primary",
                use_container_width=True,
                disabled=save_disabled
            )

            if submit_button and not save_disabled:
                try:
                    save_workflow_category_and_type(destination_category, workflow_type)

                    fresh_weekly   = get_system_setting('weekly_workflow_settings', {})
                    saved_day_keys = []

                    for (tdk, tdl, tdd) in target_days:
                        if save_scope == "Single day (selected above)":
                            day_collection_name = collection_name
                        else:
                            day_coll = f"{destination_category.lower()}_{workflow_type.lower()}"
                            if workflow_source_name:
                                day_coll += f"_{workflow_source_name.lower().replace(' ', '_').replace('-', '_')}"
                            day_coll += f"_{tdd.strftime('%Y%m%d')}"
                            day_collection_name = ''.join(c for c in day_coll if c.isalnum() or c in ['_', '-'])

                        day_automa_config: Dict[str, Any] = {
                            'config_date':          tdd.isoformat(),
                            'day_name':             tdl,
                            'day_key':              tdk,
                            'content_items':        content_items,
                            'content_type':         content_items[0]['content_type'],
                            'content_name':         content_items[0]['content_name'],
                            'source_category':      source_category.lower(),
                            'workflow_source_id':   workflow_source_id,
                            'workflow_source_name': workflow_source_name,
                            'destination_category': destination_category.lower(),
                            'workflow_type':        workflow_type.lower(),
                            'collection_name':      day_collection_name,
                            'database':             'execution_workflows',
                            'press_keys':           press_keys_cfg,
                            'click_elements':       click_elements_cfg,
                            'typing_delay':         typing_delay_cfg,
                            'updated_at':           datetime.now().isoformat()
                        }

                        day_cfg = fresh_weekly.get(tdk, {})
                        day_cfg['automa_workflow_config'] = day_automa_config
                        if 'day_name'    not in day_cfg: day_cfg['day_name']    = tdl
                        if 'day_key'     not in day_cfg: day_cfg['day_key']     = tdk
                        if 'config_date' not in day_cfg: day_cfg['config_date'] = tdd.isoformat()
                        if 'enabled'     not in day_cfg: day_cfg['enabled']     = True
                        fresh_weekly[tdk] = day_cfg
                        saved_day_keys.append(tdl)

                    update_system_setting('weekly_workflow_settings', fresh_weekly)

                    if len(saved_day_keys) == 1:
                        st.success(f"✅ Configuration saved for **{saved_day_keys[0]}**!")
                    else:
                        st.success(f"✅ Configuration saved for **{len(saved_day_keys)} days**: {', '.join(saved_day_keys)}")

                    for item in content_items:
                        st.info(f"📝 {item['label']}: {item['content_name']} ({item['content_type']})")
                    st.info(f"📂 {source_category} → {workflow_source_name}")
                    st.info(f"💾 {destination_category} → {workflow_type}")

                    if press_keys_cfg['mode'] == 'fixed':
                        st.info(f"⌨️ Press Keys: Fixed {press_keys_cfg['max_milliseconds']}ms")
                    else:
                        st.info(f"⌨️ Press Keys: {press_keys_cfg['min_milliseconds']}ms – {press_keys_cfg['max_milliseconds']}ms")
                    if click_elements_cfg['mode'] == 'fixed':
                        st.info(f"🖱️ Click Elements: Fixed {click_elements_cfg['max_milliseconds']}ms")
                    else:
                        st.info(f"🖱️ Click Elements: {click_elements_cfg['min_milliseconds']}ms – {click_elements_cfg['max_milliseconds']}ms")
                    # Typing delay always fixed
                    st.info(f"⌚ Typing Delay: Fixed {typing_delay_cfg['max_milliseconds']}ms (Automa forms node)")

                    st.balloons()

                    st.session_state.pop('templates_cache', None)
                    st.session_state['automa_form_initialized'] = False

                except Exception as e:
                    st.error(f"❌ Error saving: {e}")
                    import traceback
                    st.error(traceback.format_exc())

        # ── Refresh buttons ───────────────────────────────────────────────────
        st.markdown("---")
        rc1, rc2, rc3 = st.columns([2, 1, 1])
        with rc2:
            if st.button("🔄 Refresh Templates", key="refresh_workflow_templates"):
                st.session_state.pop('templates_cache', None)
                st.rerun()
        with rc3:
            if st.button("🔄 Refresh Categories", key="refresh_categories"):
                for k in ['templates_cache', 'template_categories_cache',
                          'execution_categories_cache', 'default_template_checked']:
                    st.session_state.pop(k, None)
                st.rerun()

        # ── Saved categories & types ──────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 📚 Saved Workflow Categories & Types")
        workflow_categories = get_system_setting('workflow_categories', {})
        if workflow_categories and isinstance(workflow_categories, dict):
            for category, types in sorted(workflow_categories.items()):
                with st.expander(f"📁 {category} ({len(types)} types)"):
                    if types:
                        for wf_type in types:
                            c1, c2 = st.columns([4, 1])
                            with c1:
                                st.write(f"• {wf_type}")
                            with c2:
                                if st.button("🗑️", key=f"delete_type_{category}_{wf_type}"):
                                    types.remove(wf_type)
                                    workflow_categories[category] = types
                                    update_system_setting('workflow_categories', workflow_categories)
                                    st.rerun()
                    else:
                        st.info("No types in this category")
                    if st.button(f"🗑️ Delete Category", key=f"delete_cat_{category}"):
                        del workflow_categories[category]
                        update_system_setting('workflow_categories', workflow_categories)
                        st.rerun()
        else:
            st.info("ℹ️ No saved workflow categories yet.")

        # ── All configured automa settings ───────────────────────────────────
        if weekly_settings:
            st.markdown("---")
            st.markdown("### 📋 All Configured Automa Settings")

            days_order = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

            for dk in days_order:
                if dk in weekly_settings:
                    day_config    = weekly_settings[dk]
                    automa_config = day_config.get('automa_workflow_config')

                    if not automa_config:
                        continue

                    config_date          = automa_config.get('config_date', 'N/A')
                    config_content_items = automa_config.get('content_items', [])
                    config_source_cat    = automa_config.get('source_category', 'N/A').title()
                    config_workflow      = automa_config.get('workflow_source_name', 'N/A')
                    config_dest_cat      = automa_config.get('destination_category', 'N/A').title()
                    config_collection    = automa_config.get('collection_name', 'N/A')
                    config_workflow_type = automa_config.get('workflow_type', 'N/A')

                    items_display = config_content_items
                    if not items_display:
                        old_ct = automa_config.get('content_type', 'N/A')
                        old_cn = automa_config.get('content_name', 'N/A')
                        if old_ct != 'N/A':
                            items_display = [{'label': 'CONTENT1', 'content_type': old_ct, 'content_name': old_cn}]

                    first_ct = items_display[0].get('content_type', 'N/A') if items_display else 'N/A'
                    first_cn = items_display[0].get('content_name', 'N/A') if items_display else 'N/A'
                    if len(items_display) > 1:
                        first_cn += f" (+{len(items_display)-1} more)"

                    with st.expander(
                        f"🤖 {day_config.get('day_name', dk.capitalize())} - {config_date} | {get_emoji_for_type(first_ct)} {first_cn}",
                        expanded=False
                    ):
                        ec1, ec2, ec3, ec4 = st.columns(4)

                        with ec1:
                            st.markdown("**📝 Content Items**")
                            for item in items_display:
                                emoji = get_emoji_for_type(item.get('content_type', 'N/A'))
                                st.write(f"{item.get('label','N/A')}: {emoji} {item.get('content_type','N/A')} - {item.get('content_name','N/A')}")
                            st.write(f"Source: {config_source_cat} → {config_workflow}")

                        with ec2:
                            st.markdown("**💾 Destination**")
                            st.write(f"Category: {config_dest_cat}")
                            st.write(f"Type: {config_workflow_type}")
                            st.write(f"Collection: {config_collection}")

                        with ec3:
                            press      = automa_config.get('press_keys', {})
                            press_mode = press.get('mode', 'range')
                            if press_mode == 'fixed':
                                st.caption(f"⌨️ Press: Fixed {press.get('max_milliseconds', 0)/1000:.2f}s")
                            else:
                                st.caption(f"⌨️ Press: {press.get('min_milliseconds',0)/1000:.2f}s – {press.get('max_milliseconds',0)/1000:.2f}s")

                            click      = automa_config.get('click_elements', {})
                            click_mode = click.get('mode', 'range')
                            if click_mode == 'fixed':
                                st.caption(f"🖱️ Click: Fixed {click.get('max_milliseconds', 0)/1000:.2f}s")
                            else:
                                st.caption(f"🖱️ Click: {click.get('min_milliseconds',0)/1000:.2f}s – {click.get('max_milliseconds',0)/1000:.2f}s")

                            # Typing delay always fixed
                            typing = automa_config.get('typing_delay', {})
                            st.caption(f"⌚ Typing: Fixed {typing.get('max_milliseconds', 0)}ms (forms node)")

                        with ec4:
                            if st.button("✏️ Edit", key=f"edit_automa_{dk}", use_container_width=True):
                                st.session_state[f'editing_automa_{dk}'] = True
                                st.rerun()
                            if st.button("🗑️ Delete", key=f"delete_automa_{dk}", use_container_width=True):
                                fresh = get_system_setting('weekly_workflow_settings', {})
                                if 'automa_workflow_config' in fresh.get(dk, {}):
                                    del fresh[dk]['automa_workflow_config']
                                    update_system_setting('weekly_workflow_settings', fresh)
                                    st.rerun()

                        # ── Inline edit form ──────────────────────────────────
                        if st.session_state.get(f'editing_automa_{dk}'):
                            st.markdown("---")
                            st.markdown(f"**✏️ Editing {dk.title()} automa configuration**")

                            with st.form(key=f"edit_automa_form_{dk}"):
                                st.markdown("**⌨️ Press Keys Configuration**")
                                saved_press      = automa_config.get('press_keys', {})
                                saved_press_mode = saved_press.get('mode', 'range')
                                if saved_press_mode not in ('range', 'fixed'):
                                    saved_press_mode = 'range'

                                edit_press_mode = st.radio(
                                    "Press Keys Mode",
                                    options=["Range", "Fixed"],
                                    index=0 if saved_press_mode == 'range' else 1,
                                    horizontal=True,
                                    key=f"edit_press_mode_{dk}"
                                )

                                if edit_press_mode == "Range":
                                    ep1, ep2 = st.columns(2)
                                    with ep1:
                                        edit_press_min = st.number_input(
                                            "Min Press (ms)", min_value=0, max_value=60000,
                                            value=int(saved_press.get('min_milliseconds', 0)),
                                            step=1000, key=f"edit_press_min_{dk}"
                                        )
                                    with ep2:
                                        edit_press_max = st.number_input(
                                            "Max Press (ms)", min_value=0, max_value=60000,
                                            value=int(saved_press.get('max_milliseconds', 10000)),
                                            step=1000, key=f"edit_press_max_{dk}"
                                        )
                                    edit_press_cfg = {
                                        'mode': 'range',
                                        'min_milliseconds': edit_press_min,
                                        'max_milliseconds': edit_press_max
                                    }
                                else:
                                    saved_fixed_press = saved_press.get('max_milliseconds', 1000) if saved_press_mode == 'fixed' else 1000
                                    edit_press_fixed = st.number_input(
                                        "Fixed Press Value (ms)", min_value=0, max_value=60000,
                                        value=int(saved_fixed_press),
                                        step=1000, key=f"edit_press_fixed_{dk}"
                                    )
                                    edit_press_cfg = {
                                        'mode': 'fixed',
                                        'min_milliseconds': edit_press_fixed,
                                        'max_milliseconds': edit_press_fixed
                                    }

                                st.markdown("---")

                                st.markdown("**🖱️ Click Elements Configuration**")
                                saved_click      = automa_config.get('click_elements', {})
                                saved_click_mode = saved_click.get('mode', 'range')
                                if saved_click_mode not in ('range', 'fixed'):
                                    saved_click_mode = 'range'

                                edit_click_mode = st.radio(
                                    "Click Elements Mode",
                                    options=["Range", "Fixed"],
                                    index=0 if saved_click_mode == 'range' else 1,
                                    horizontal=True,
                                    key=f"edit_click_mode_{dk}"
                                )

                                if edit_click_mode == "Range":
                                    ec_1, ec_2 = st.columns(2)
                                    with ec_1:
                                        edit_click_min = st.number_input(
                                            "Min Click (ms)", min_value=0, max_value=60000,
                                            value=int(saved_click.get('min_milliseconds', 0)),
                                            step=1000, key=f"edit_click_min_{dk}"
                                        )
                                    with ec_2:
                                        edit_click_max = st.number_input(
                                            "Max Click (ms)", min_value=0, max_value=60000,
                                            value=int(saved_click.get('max_milliseconds', 10000)),
                                            step=1000, key=f"edit_click_max_{dk}"
                                        )
                                    edit_click_cfg = {
                                        'mode': 'range',
                                        'min_milliseconds': edit_click_min,
                                        'max_milliseconds': edit_click_max
                                    }
                                else:
                                    saved_fixed_click = saved_click.get('max_milliseconds', 1000) if saved_click_mode == 'fixed' else 1000
                                    edit_click_fixed = st.number_input(
                                        "Fixed Click Value (ms)", min_value=0, max_value=60000,
                                        value=int(saved_fixed_click),
                                        step=1000, key=f"edit_click_fixed_{dk}"
                                    )
                                    edit_click_cfg = {
                                        'mode': 'fixed',
                                        'min_milliseconds': edit_click_fixed,
                                        'max_milliseconds': edit_click_fixed
                                    }

                                st.markdown("---")

                                # ── Typing Delay — fixed only in edit form ────
                                st.markdown("**⌚ Typing Delay Configuration**")
                                st.caption(
                                    "Automa 'Typing delay (millisecond)' — applied to the forms node only. "
                                    "0 disables it."
                                )
                                saved_typing           = automa_config.get('typing_delay', {})
                                saved_fixed_typing_val = int(saved_typing.get('max_milliseconds', 80))

                                edit_typing_fixed = st.number_input(
                                    "Typing Delay (ms)",
                                    min_value=0, max_value=60000,
                                    value=saved_fixed_typing_val,
                                    step=1000,
                                    key=f"edit_typing_fixed_{dk}",
                                    help="Automa 'Typing delay (millisecond)' — 0 disables it."
                                )
                                st.caption(
                                    f"💡 Will set Automa typing delay to exactly **{edit_typing_fixed}ms** "
                                    f"(**{edit_typing_fixed/1000:.2f}s**)"
                                )
                                edit_typing_cfg = {
                                    'mode': 'fixed',
                                    'min_milliseconds': edit_typing_fixed,
                                    'max_milliseconds': edit_typing_fixed,
                                }

                                st.markdown("---")

                                st.markdown("**📝 Content Items**")
                                edit_content_items = []
                                for ei, item in enumerate(items_display):
                                    eci1, eci2 = st.columns(2)
                                    with eci1:
                                        curr_ct = item.get('content_type', available_prompt_types[0])
                                        ct_idx  = available_prompt_types.index(curr_ct) if curr_ct in available_prompt_types else 0
                                        edit_ct = st.selectbox(
                                            f"{item.get('label','CONTENT')} Type",
                                            options=available_prompt_types,
                                            index=ct_idx,
                                            key=f"edit_automa_ct_{dk}_{ei}"
                                        )
                                    with eci2:
                                        edit_cn = st.text_input(
                                            f"{item.get('label','CONTENT')} Name",
                                            value=item.get('content_name', ''),
                                            key=f"edit_automa_cn_{dk}_{ei}"
                                        )
                                    edit_content_items.append({
                                        'label':        item.get('label', f'CONTENT{ei+1}'),
                                        'content_type': edit_ct,
                                        'content_name': edit_cn
                                    })

                                save_col, cancel_col = st.columns(2)
                                with save_col:
                                    save_edit = st.form_submit_button(
                                        "💾 Save Changes", type="primary", use_container_width=True
                                    )
                                with cancel_col:
                                    cancel_edit = st.form_submit_button(
                                        "❌ Cancel", use_container_width=True
                                    )

                            if save_edit:
                                try:
                                    fresh = get_system_setting('weekly_workflow_settings', {})
                                    fresh[dk]['automa_workflow_config'].update({
                                        'content_items':  edit_content_items,
                                        'content_type':   edit_content_items[0]['content_type'],
                                        'content_name':   edit_content_items[0]['content_name'],
                                        'press_keys':     edit_press_cfg,
                                        'click_elements': edit_click_cfg,
                                        'typing_delay':   edit_typing_cfg,
                                        'updated_at':     datetime.now().isoformat()
                                    })
                                    update_system_setting('weekly_workflow_settings', fresh)
                                    st.session_state[f'editing_automa_{dk}'] = False
                                    st.success(f"✅ {dk.title()} updated!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Failed to save: {e}")

                            if cancel_edit:
                                st.session_state[f'editing_automa_{dk}'] = False
                                st.rerun()

    except Exception as e:
        st.error(f"❌ Error loading automa workflow settings: {e}")
        import traceback
        st.error(traceback.format_exc())
