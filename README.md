Given you have AWS SQS setup that routes bounce notifications to SNS topic,
bouncehandler can be used as a http endpoint subscriber for this SNS topic.
This program can run custom MySQL queries for received bounces to mark bad
emails in database.


	Usage of bouncehandler:
	  -addr string
		address to listen at (default "localhost:8080")
	  -config string
		configuration file (default "mapping.json")
	  -pass string
		basic auth password
	  -user string
		basic auth user

	Configuration file should be in json format, it is a mapping between sender
	emails and objects with two fields:

	dsn — MySQL Data Source Name in the following format:

		[username[:password]@][protocol[(address)]]/dbname

	sql — MySQL query with single ? placeholder that will be replaced by recipient's
	email from the bounce notification.

	Example:

	{
		"news@example.com": {
			"dsn": "user:password@tcp(192.168.0.1:3306)/news",
			"sql": "delete from subscribers where email=?"
		},
		"notifications@example.com": {
			"dsn": "user:password@tcp(192.168.0.1:3306)/forum",
			"sql": "update users set notify=0 where email=?"
		}
	}

	You may also optionally have one "catch-all" record in a mapping with key value
	"*": it would be used if sender listed in bounce notification did not match any
	other records.
	exit status 2
