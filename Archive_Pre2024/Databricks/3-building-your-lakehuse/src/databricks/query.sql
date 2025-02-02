SELECT sl.zipCode, sl.latitude, sl.longitude, sum(pc.powerConsumptedKwh) as totalPowerUsed
FROM gold.production_factpowerconsumption pc
JOIN gold.reference_dimservicelocation sl on (sl.sid = pc.serviceLocationSid)
GROUP BY sl.zipCode, sl.latitude, sl.longitude
ORDER BY totalPowerUsed DESC
LIMIT 10;