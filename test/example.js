var db = require("../lib/mongopromise").create({
    db: "ecostudio",
    host: "127.0.0.1",
    port: 27017,
    poolSize:100
});

var collection = db.collection("users");

collection.find({}).then(function (data) {
    console.log(JSON.stringify(data));
})

