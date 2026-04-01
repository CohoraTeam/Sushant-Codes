#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import yaml
import warnings
from google.cloud import bigquery

# Suppress BigQuery storage warnings if running locally without it
warnings.filterwarnings("ignore", category=UserWarning)

def get_engagement_scores(bq_client: bigquery.Client, config: dict, tenant_config: dict) -> pd.DataFrame:
    """
    Executes the 90-Day Rolling Engagement SQL in BigQuery dynamically using tenant weights and YAML config.
    """
    tenant_project_id = config['tenant']['project_id']
    dataset = config['tenant']['cohora_dataset']
    tbl = config['tables']
    weights = tenant_config.get("engagement_weights", {})

    sql_query = f"""
        WITH window_dates AS (
            SELECT DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AS start_date
        ),

        raw_events AS (
            SELECT l.profile_id, DATE(l.created_at) AS event_date, 'Like' AS Event_Name, {weights.get('like', 1)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['like']}` l 
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON l.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(l.created_at) >= w.start_date AND l.external_type = 'POST'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT c.profile_id, DATE(c.created_at) AS event_date, 'Comment' AS Event_Name, {weights.get('comment', 5)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['comment']}` c 
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON c.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(c.created_at) >= w.start_date AND c.external_type = 'POST'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT s.profile_id, DATE(s.created_at) AS event_date, 'Share' AS Event_Name, {weights.get('share', 6)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['push_out']}` s 
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON s.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(s.created_at) >= w.start_date AND s.external_type = 'POST'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT p.id AS profile_id, DATE(p.last_updated_at) AS event_date, 'Complete profile' AS Event_Name, {weights.get('profile_completion', 4)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['profile']}` p 
            CROSS JOIN window_dates w
            WHERE DATE(p.last_updated_at) >= w.start_date AND p.name IS NOT NULL AND p.email IS NOT NULL AND p.photo_asset_id IS NOT NULL
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT l.profile_id, DATE(l.created_at) AS event_date, 'Contest Like' AS Event_Name, {weights.get('contest_like', 4)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['like']}` l 
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON l.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(l.created_at) >= w.start_date AND l.external_type = 'CONTEST'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT c.profile_id, DATE(c.created_at) AS event_date, 'Contest Comments' AS Event_Name, {weights.get('contest_comment', 5)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['comment']}` c 
            JOIN `{tenant_project_id}.{dataset}.{tbl['contest_entry']}` ce ON c.external_id = ce.id 
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON c.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(c.created_at) >= w.start_date
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT pa.profile_id, DATE(pa.created_at) AS event_date, 'Email click' AS Event_Name, {weights.get('email_click', 4)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['profile_activity']}` pa 
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON pa.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(pa.created_at) >= w.start_date AND pa.type IN ('MESSAGE_CLICKED', 'MESSAGE_READ')
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT pa.profile_id, DATE(pa.created_at) AS event_date, 'Scratch a card' AS Event_Name, {weights.get('scratch_card', 5)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['profile_activity']}` pa 
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON pa.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(pa.created_at) >= w.start_date AND pa.type = 'SCRATCH_CARD_OPENED'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT pa_asset.profile_id, DATE(pa_asset.created_at) AS event_date, 'Creator post' AS Event_Name, {weights.get('creator_post', 6)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['post_asset']}` pa_asset
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON pa_asset.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(pa_asset.created_at) >= w.start_date
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT po.profile_id, DATE(po.created_at) AS event_date, 'Poll' AS Event_Name, {weights.get('poll', 6)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['post']}` po
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON po.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(po.created_at) >= w.start_date AND po.post_type = 'POLL'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT po.profile_id, DATE(po.created_at) AS event_date, 'Survey' AS Event_Name, {weights.get('survey', 7)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['post']}` po
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON po.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(po.created_at) >= w.start_date AND po.post_type = 'SURVEY'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT po.profile_id, DATE(po.created_at) AS event_date, 'Quiz' AS Event_Name, {weights.get('quiz', 7)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['post']}` po
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON po.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(po.created_at) >= w.start_date AND po.post_type = 'QUIZ'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT po.profile_id, DATE(po.created_at) AS event_date, 'Contest participation' AS Event_Name, {weights.get('contest_participation', 7)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['post']}` po
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON po.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(po.created_at) >= w.start_date AND po.post_type = 'CONTEST'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT p.id AS profile_id, DATE(p.created_at) AS event_date, 'Refer a friend' AS Event_Name, {weights.get('refer_a_friend', 9)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['profile']}` p
            CROSS JOIN window_dates w
            WHERE DATE(p.created_at) >= w.start_date AND p.referred_profile_id IS NOT NULL
             AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT pa.profile_id, DATE(pa.created_at) AS event_date, 'Redeem discount' AS Event_Name, {weights.get('redeem_discount', 9)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['profile_activity']}` pa
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON pa.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(pa.created_at) >= w.start_date AND pa.type = 'VOUCHER_PURCHASED'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT pa.profile_id, DATE(pa.created_at) AS event_date, 'Purchase from recommendation' AS Event_Name, {weights.get('purchase_recommendation', 10)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['profile_activity']}` pa
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON pa.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(pa.created_at) >= w.start_date AND pa.type = 'SHOPIFY_DISCOUNT_PURCHASED'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT po.profile_id, DATE(po.created_at) AS event_date, 'UGC' AS Event_Name, {weights.get('ugc', 9)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['post']}` po
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON po.profile_id = p.id
            CROSS JOIN window_dates w
            WHERE DATE(po.created_at) >= w.start_date AND po.post_type = 'POST'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)

            UNION ALL
            SELECT w.profile_id, DATE(w.created_at) AS event_date, 'Redeem loyalty points' AS Event_Name, {weights.get('redeem_loyalty', 8)} AS Base_Weight
            FROM `{tenant_project_id}.{dataset}.{tbl['wallet_transaction']}` w
            JOIN `{tenant_project_id}.{dataset}.{tbl['profile']}` p ON w.profile_id = p.id
            CROSS JOIN window_dates wd
            WHERE DATE(w.created_at) >= wd.start_date 
              AND w.state = 'SUCCESS'
              AND (p.tenant_admin = FALSE OR p.tenant_admin IS NULL)
        ),

        event_multipliers AS (
            SELECT 
                profile_id,
                Event_Name,
                Base_Weight,
                DATE_DIFF(CURRENT_DATE(), event_date, DAY) AS days_since,

                CASE 
                    WHEN DATE_DIFF(CURRENT_DATE(), event_date, DAY) <= 3 THEN 1.5
                    WHEN DATE_DIFF(CURRENT_DATE(), event_date, DAY) <= 14 THEN 1.2
                    WHEN DATE_DIFF(CURRENT_DATE(), event_date, DAY) <= 30 THEN 1.0
                    WHEN DATE_DIFF(CURRENT_DATE(), event_date, DAY) <= 90 THEN 0.7
                    ELSE 0.4 
                END AS recency_mult,

                COUNT(*) OVER (PARTITION BY profile_id, Event_Name) AS event_frequency

            FROM raw_events
        ),

        scored_events AS (
            SELECT 
                profile_id,
                Event_Name,
                Base_Weight,
                recency_mult,
                event_frequency,

                (Base_Weight * recency_mult * CASE 
                    WHEN event_frequency = 1 THEN 1.0
                    WHEN event_frequency BETWEEN 2 AND 3 THEN 1.2
                    WHEN event_frequency BETWEEN 4 AND 7 THEN 1.4
                    WHEN event_frequency BETWEEN 8 AND 15 THEN 1.6
                    ELSE 1.7 
                 END) AS exact_event_score

            FROM event_multipliers
        ),

        user_raw_scores AS (
            SELECT 
                profile_id,
                SUM(exact_event_score) AS raw_score
            FROM scored_events
            GROUP BY profile_id
        ),

        global_min_max AS (
            SELECT 
                MIN(raw_score) AS min_score,
                MAX(raw_score) AS max_score
            FROM user_raw_scores
        )

        SELECT 
            u.profile_id AS profile_id,
            ROUND(u.raw_score, 2) AS raw_engagement_score,

            ROUND(
                100 * (u.raw_score - g.min_score) / NULLIF((g.max_score - g.min_score), 0), 
            0) AS ces_score,

            CASE 
                WHEN 100 * (u.raw_score - g.min_score) / NULLIF((g.max_score - g.min_score), 0) >= 81 THEN 'Very High'
                WHEN 100 * (u.raw_score - g.min_score) / NULLIF((g.max_score - g.min_score), 0) >= 61 THEN 'High'
                WHEN 100 * (u.raw_score - g.min_score) / NULLIF((g.max_score - g.min_score), 0) >= 41 THEN 'Medium'
                WHEN 100 * (u.raw_score - g.min_score) / NULLIF((g.max_score - g.min_score), 0) >= 21 THEN 'Low'
                ELSE 'Very Low'
            END AS engagement_level

        FROM user_raw_scores u
        CROSS JOIN global_min_max g
        ORDER BY ces_score DESC;
    """

    return bq_client.query(sql_query).to_dataframe()


# ==========================================
# 2. MAIN EXECUTION PIPELINE
# ==========================================
def run_segmentation(config_path: str, tenant_config: dict) -> pd.DataFrame:
    """
    Main execution pipeline. Returns the fully joined and mapped Customer 360 DataFrame.
    """
    # --- 1. Load Configurations ---
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    billing_project = config['gcp']['billing_project']
    tenant_project_id = config['tenant']['project_id']
    dataset = config['tenant']['cohora_dataset']
    tbl = config['tables']

    bq_client = bigquery.Client(project=billing_project)

    # --- 2. Fetch Transaction Data ---
    query = f"""
       SELECT id, profile_id, processed_at, current_total_price 
       FROM `{tenant_project_id}.{dataset}.{tbl['order']}` 
    """
    try:
        df = bq_client.query(query).to_dataframe()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch transaction data: {e}")

    if df.empty:
        return pd.DataFrame()

    # --- 3. Process Transactions (Customer 360) ---
    df['processed_at'] = pd.to_datetime(df['processed_at'], errors='coerce')
    df['current_total_price'] = pd.to_numeric(df['current_total_price'], errors='coerce').fillna(0)
    analysis_date = df['processed_at'].max()

    customer_metrics = df.groupby('profile_id').agg(
        first_purchase_date=('processed_at', 'min'),
        last_purchase_date=('processed_at', 'max'),
        frequency=('id', 'count'),
        total_revenue=('current_total_price', 'sum')
    ).reset_index()

    customer_metrics['recency_days'] = (analysis_date - customer_metrics['last_purchase_date']).dt.days
    customer_metrics['lifespan'] = (analysis_date - customer_metrics['first_purchase_date']).dt.days

    # --- 4. Apply Audience Type ---
    cycle_days = tenant_config["expected_cycle_days"]

    def assign_audience(row):
        if row['lifespan'] <= cycle_days and row['frequency'] == 1:
            return 'New'
        elif row['recency_days'] > cycle_days:
            return 'Inactive'
        else:
            return 'Returning'

    customer_metrics['audience_type'] = customer_metrics.apply(assign_audience, axis=1)

    # --- 5. Transaction Score Tiers ---
    p_bottom = tenant_config["bottom_percentile"]
    p_top = tenant_config["top_percentile"]

    f_low_val = customer_metrics['frequency'].quantile(p_bottom)
    f_high_val = customer_metrics['frequency'].quantile(p_top)
    r_low_val = customer_metrics['recency_days'].quantile(p_bottom)
    r_high_val = customer_metrics['recency_days'].quantile(p_top)

    if pd.notna(tenant_config.get("revenue_high_override")):
        m_high_val = tenant_config["revenue_high_override"]
    else:
        m_high_val = customer_metrics['total_revenue'].quantile(p_top)

    if pd.notna(tenant_config.get("revenue_low_override")):
        m_low_val = tenant_config["revenue_low_override"]
    else:
        m_low_val = customer_metrics['total_revenue'].quantile(p_bottom)

    def get_tier(val, low_thresh, high_thresh, reverse=False):
        if reverse: 
            if val <= low_thresh: return 'High'
            elif val >= high_thresh: return 'Low'
            else: return 'Medium'
        else:
            if val >= high_thresh: return 'High'
            elif val <= low_thresh: return 'Low'
            else: return 'Medium'

    customer_metrics['f_tier'] = customer_metrics['frequency'].apply(lambda x: get_tier(x, f_low_val, f_high_val))
    customer_metrics['m_tier'] = customer_metrics['total_revenue'].apply(lambda x: get_tier(x, m_low_val, m_high_val))
    customer_metrics['r_tier'] = customer_metrics['recency_days'].apply(lambda x: get_tier(x, r_low_val, r_high_val, reverse=True))

    # --- 6. Macro Segment & Purchase Tier ---
    vip_min_freq = tenant_config["vip_purchase_min"]

    def assign_macro(row):
        if row['m_tier'] == 'High' and row['frequency'] >= vip_min_freq:
            return 'VIP'
        elif row['m_tier'] == 'Low' or row['r_tier'] == 'Low': 
            return 'At Risk'
        else:
            return 'Loyal'

    customer_metrics['macro_segment'] = customer_metrics.apply(assign_macro, axis=1)

    def assign_purchase_tier(freq):
        if freq >= vip_min_freq:
            return f'{vip_min_freq}+'
        else:
            return str(int(freq))

    customer_metrics['purchase_tier'] = customer_metrics['frequency'].apply(assign_purchase_tier)

    # --- 7. Calculate CLTV and pCLTV ---
    customer_metrics['historical_cltv'] = customer_metrics['total_revenue']
    customer_metrics['aov'] = customer_metrics['total_revenue'] / customer_metrics['frequency']
    customer_metrics['age_years'] = np.maximum(customer_metrics['lifespan'], 1) / 365.0
    customer_metrics['annual_purchase_rate'] = customer_metrics['frequency'] / customer_metrics['age_years']
    lifespan_years = tenant_config.get("expected_lifespan_years", 3)

    customer_metrics['predicted_cltv'] = (
        customer_metrics['aov'] * customer_metrics['annual_purchase_rate'] * lifespan_years
    )

    def apply_churn_penalty(row):
        if row['recency_days'] > (cycle_days * 2):
            return row['predicted_cltv'] * 0.10
        elif row['recency_days'] > cycle_days:
            return row['predicted_cltv'] * 0.50
        else:
            return row['predicted_cltv']

    customer_metrics['predicted_cltv'] = customer_metrics.apply(apply_churn_penalty, axis=1)

    customer_metrics['predicted_cltv'] = customer_metrics['predicted_cltv'].round(2)
    customer_metrics['historical_cltv'] = customer_metrics['historical_cltv'].round(2)
    customer_metrics['aov'] = customer_metrics['aov'].round(2)
    customer_metrics = customer_metrics.drop(columns=['age_years', 'annual_purchase_rate'])

    customer_metrics['name_of_segment'] = (
        customer_metrics['macro_segment'] + '_' + 
        customer_metrics['audience_type'] + '_Purchase ' + 
        customer_metrics['purchase_tier']
    )

    # --- 8. Fetch Engagement Data & Merge ---
    df_engagement = get_engagement_scores(bq_client, config, tenant_config)

    df_combined = pd.merge(
        customer_metrics, 
        df_engagement, 
        on='profile_id', 
        how='left'
    )

    df_combined['engagement_level'] = df_combined['engagement_level'].fillna('Not Active') 
    df_combined['ces_score'] = df_combined['ces_score'].fillna(0)
    df_combined['raw_engagement_score'] = df_combined['raw_engagement_score'].fillna(0)

    # --- 9. Assign Persona Overlay ---
    def assign_persona(row):
        macro = row['macro_segment']
        audience = row['audience_type']
        eng_level = row['engagement_level']

        is_highly_engaged = eng_level in ['Very High', 'High']
        is_low_engaged = eng_level in ['Very Low', 'Not Active', 'Medium', 'Low']

        if macro == 'VIP' and audience == 'Returning' and is_highly_engaged:
            return 'Champions'
        elif macro == 'VIP' and is_low_engaged:
            return 'High Value | Silent'
        elif macro == 'Loyal' and audience == 'Returning' and is_highly_engaged:
            return 'Community Builders'
        elif macro == 'At Risk' and audience == 'Returning' and is_highly_engaged:
            return 'Emotionally Engaged | Not Buying'
        elif macro == 'At Risk' and audience == 'Inactive' and is_low_engaged:
            return 'Cold'
        else:
            return f"{macro} {audience} ({eng_level})"

    df_combined['persona_overlay'] = df_combined.apply(assign_persona, axis=1)

    # --- 10. Calculate Single-Value Integer Thresholds (Min/Max) ---
    f_low = int(round(customer_metrics['frequency'].quantile(p_bottom)))
    f_high = int(round(customer_metrics['frequency'].quantile(p_top)))

    m_low = int(round(customer_metrics['total_revenue'].quantile(p_bottom)))
    m_high = int(round(customer_metrics['total_revenue'].quantile(p_top)))

    r_low = int(round(customer_metrics['recency_days'].quantile(p_bottom)))
    r_high = int(round(customer_metrics['recency_days'].quantile(p_top)))

    def map_f_min_max(tier):
        if tier == 'High': return pd.Series([f_high, None])
        elif tier == 'Medium': return pd.Series([f_low, f_high - 1])
        else: return pd.Series([0, f_low - 1])

    def map_m_min_max(tier):
        if tier == 'High': return pd.Series([m_high, None])
        elif tier == 'Medium': return pd.Series([m_low, m_high - 1])
        else: return pd.Series([0, m_low - 1])

    def map_r_min_max(tier):
        if tier == 'High': return pd.Series([0, r_low]) 
        elif tier == 'Medium': return pd.Series([r_low + 1, r_high])
        else: return pd.Series([r_high + 1, None]) 

    def map_ces_min_max(level):
        mapping = {
            'Very High': (81, 100),
            'High': (61, 80),
            'Medium': (41, 60), 
            'Low': (21, 40),
            'Very Low': (1, 20),
            'Not Active': (0, 0)
        }
        return pd.Series(mapping.get(level, (None, None)))

    df_combined[['frequency_min', 'frequency_max']] = df_combined['f_tier'].apply(map_f_min_max)
    df_combined[['monetary_min', 'monetary_max']] = df_combined['m_tier'].apply(map_m_min_max)
    df_combined[['recency_min', 'recency_max']] = df_combined['r_tier'].apply(map_r_min_max)
    df_combined[['ces_min', 'ces_max']] = df_combined['engagement_level'].apply(map_ces_min_max)

    int_cols = [
        'frequency_min', 'frequency_max', 
        'monetary_min', 'monetary_max', 
        'recency_min', 'recency_max', 
        'ces_min', 'ces_max'
    ]
    for col in int_cols:
        df_combined[col] = df_combined[col].astype('Int64')

    return df_combined


# In[ ]:




