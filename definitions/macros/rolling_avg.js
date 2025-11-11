// Replicates the Terraform/Jinja rolling average helper using Dataform's JS macro interface.
module.exports = {
  rolling_avg(expr, dateCol, partitionBy, days = 28) {
    const windowDays = Number(days) - 1;
    return `AVG(${expr}) OVER (
      PARTITION BY ${partitionBy}
      ORDER BY ${dateCol}
      RANGE BETWEEN INTERVAL ${windowDays} DAY PRECEDING AND CURRENT ROW
    )`;
  }
};
