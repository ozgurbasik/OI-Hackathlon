
WITH campaign_performance AS (
    SELECT 
        c.id as campaign_id,
        c.name as campaign_name,
        c.start_date,
        c.end_date,
        c.budget,
        c.channel,
        COUNT(cl.id) as total_clicks,
        COUNT(DISTINCT cl.user_id) as unique_users,
        SUM(CASE WHEN cl.converted = 1 THEN 1 ELSE 0 END) as conversions,
        SUM(cl.cost_per_click) as total_cost,
        AVG(cl.cost_per_click) as avg_cpc
    FROM campaigns c
    LEFT JOIN clicks cl ON c.id = cl.campaign_id
    WHERE c.start_date >= DATEADD(month, -12, GETDATE())
    GROUP BY c.id, c.name, c.start_date, c.end_date, c.budget, c.channel
),
conversion_metrics AS (
    SELECT 
        cp.campaign_id,
        cp.campaign_name,
        cp.channel,
        cp.total_clicks,
        cp.unique_users,
        cp.conversions,
        cp.total_cost,
        cp.budget,
        CASE 
            WHEN cp.total_clicks > 0 THEN (cp.conversions * 100.0 / cp.total_clicks)
            ELSE 0
        END as conversion_rate,
        CASE 
            WHEN cp.conversions > 0 THEN (cp.total_cost / cp.conversions)
            ELSE 0
        END as cost_per_conversion,
        ((cp.budget - cp.total_cost) * 100.0 / cp.budget) as budget_utilization
    FROM campaign_performance cp
),
channel_analysis AS (
    SELECT 
        cm.channel,
        COUNT(cm.campaign_id) as campaign_count,
        SUM(cm.total_clicks) as channel_clicks,
        SUM(cm.conversions) as channel_conversions,
        SUM(cm.total_cost) as channel_cost,
        AVG(cm.conversion_rate) as avg_conversion_rate
    FROM conversion_metrics cm
    GROUP BY cm.channel
)
SELECT 
    cm.campaign_id,
    cm.campaign_name,
    cm.channel,
    cm.total_clicks,
    cm.unique_users,
    cm.conversions,
    cm.conversion_rate,
    cm.total_cost,
    cm.cost_per_conversion,
    cm.budget_utilization,
    ca.channel_clicks,
    ca.channel_conversions,
    CASE 
        WHEN cm.conversion_rate >= 5.0 THEN 'EXCELLENT'
        WHEN cm.conversion_rate >= 2.0 THEN 'GOOD'
        WHEN cm.conversion_rate >= 1.0 THEN 'AVERAGE'
        ELSE 'POOR'
    END as performance_rating
FROM conversion_metrics cm
LEFT JOIN channel_analysis ca ON cm.channel = ca.channel
ORDER BY cm.conversion_rate DESC;

-- Channel summary
WITH channel_summary AS (
    SELECT 
        c.channel,
        COUNT(c.id) as campaign_count,
        AVG(c.budget) as avg_budget,
        SUM(c.budget) as total_budget
    FROM campaigns c
    WHERE c.start_date >= DATEADD(month, -12, GETDATE())
    GROUP BY c.channel
)
SELECT 
    cs.channel,
    cs.campaign_count,
    cs.avg_budget,
    cs.total_budget
FROM channel_summary cs
ORDER BY cs.total_budget DESC;