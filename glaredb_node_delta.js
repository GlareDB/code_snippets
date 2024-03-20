> npm install nodejs-polars
> npm install @glaredb/glaredb

const pl = require("nodejs-polars");
const glaredb = require("@glaredb/glaredb");
const con = await glaredb.connect();
const df = await (
	await con.sql(
		"select * from delta_scan('path/to/delta/table')",
	)
).toPolars();

console.log(df);
