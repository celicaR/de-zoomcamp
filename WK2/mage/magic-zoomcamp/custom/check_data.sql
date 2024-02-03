-- Docs: https://docs.mage.ai/guides/sql-blocks
SELECT vendor_id, count(*) from mage.green_taxi group by vendor_id