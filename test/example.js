var db = require("../lib/mongopromise").create({
	db: "history",
	host: "127.0.0.1",
	port: 27017
});

var collection = db.collection("errorlog");

collection.find({ retailer : "groceries.asda.com"}).then(function(data) {
	console.log(JSON.stringify(data));
})

