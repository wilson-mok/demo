-- Our data is in Q1 2024

SELECT
  AVG(total_profit) AS avg_profit_q1
FROM
  (
    SELECT
      invoices.InvoiceID,
      SUM(invlines.LineProfit) AS total_profit
    FROM
      sample_invoices_data AS invoices
    JOIN sample_invoice_lines_data AS invlines ON (invoices.InvoiceID = invlines.InvoiceID)
    WHERE
      invoices.InvoiceDate BETWEEN '2024-01-01' AND '2024-03-31'
    GROUP BY
      invoices.InvoiceID
  )