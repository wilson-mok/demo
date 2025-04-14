-- Our data is in Q1 2024

SELECT
    EXTRACT(YEAR FROM invoices.InvoiceDate) AS year,
    EXTRACT(MONTH FROM invoices.InvoiceDate) AS month,
    invlines.LineProfit AS profit
FROM
    sample_invoices_data AS invoices
JOIN sample_invoice_lines_data AS invlines ON (invoices.InvoiceID = invlines.InvoiceID)
WHERE
    invoices.InvoiceDate BETWEEN '2024-01-01' AND '2024-03-31';