influx bucket create --name WETH --schema-type explicit
influx bucket create --name AXS --schema-type explicit
influx bucket create --name SLP --schema-type explicit
influx bucket create --name GATEWAY --schema-type explicit
influx bucket create --name KATANA --schema-type explicit
influx bucket create --name BREEDING --schema-type explicit

# --name is where you specify the measurement name as a string
influx bucket-schema create --bucket WETH --name value --columns-file influxconfig.json
influx bucket-schema create --bucket AXS --name value --columns-file influxconfig.json
influx bucket-schema create --bucket SLP --name value --columns-file influxconfig.json
influx bucket-schema create --bucket GATEWAY --name value --columns-file influxconfig.json
influx bucket-schema create --bucket KATANA --name value --columns-file influxconfig.json
influx bucket-schema create --bucket BREEDING --name value --columns-file influxconfig.json