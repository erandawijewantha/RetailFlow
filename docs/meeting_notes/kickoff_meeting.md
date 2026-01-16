# Kickoff Meeting Notes
Date: [Today's Date]

## Attendees
- Tech Lead (You)
- Data Architect (You)
- Data Engineer (You)

## Discussion Points

### 1. What data do we have?
**Question:** What are our data sources and their characteristics?

**Answer:**
- Orders: ~100K records/day, CSV format, arrives at midnight
- Products: ~50K products, changes infrequently
- Customers: ~1M customers, need to track address changes (SCD2)
- Clickstream: ~10M events/day (future phase)

**Decision:** Start with Orders, Products, Customers. Add clickstream later.

### 2. What business questions need answering?
- What are daily/weekly/monthly sales?
- Who are our top customers?
- Which products are performing well?
- What's the customer retention rate?
- What's the average order value by segment?

### 3. What's the data freshness requirement?
**Business:** "We need daily reports by 8 AM"
**Decision:** Batch processing, run at 2 AM, complete by 6 AM.

### 4. Who will consume this data?
- Sales team: Daily dashboards
- Marketing: Customer segmentation
- Finance: Monthly revenue reports
- Executive: KPI scorecards

**Decision:** Power BI as primary BI tool (company standard)

## Action Items
- [ ] Create architecture diagram (Data Architect)
- [ ] Set up Git repository (Tech Lead)
- [ ] Document data source access requirements (Data Engineer)

## Next Meeting
Architecture Review - [Date + 2 days]