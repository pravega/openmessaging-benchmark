

Running test 2 of 31 (21 min ago)
	Stream 1 (21 min ago)
		Running query32.sql (2 min ago)
			Map 1/300, reduce 2 4/100 (1 sec ago)
	Stream 2 (21 min ago)
		Running query1.sql (5 min ago)

Running test 2 of 31 (21 min ago)
	Running terasort (2 min ago)
		Map 2%, reduce 0% (20 sec ago)

Current_status:
	status_text: Running test 2 of 31
 	last_update_time: 9:39PM
 	children:
		'1':
			status_text: ‘Running query32’
			last_update_time: xxx
			children:
				'':
					status_text: 'Map 1/300, reduce 2 4/100'
					last_update_time: xxx
					children: {}
		'2':
			status_text: ‘Running query32’
			last_update_time: xxx
			children: {}

	
	
	

5 Most recent errors:
Running test 2 of 31 (21 min ago)
		Stream 1 (21 min ago)
			Running query32.sql (2 min ago)
				EXCEPTION: bad sql
Most recent results:
Running test 2 of 31 (21 min ago)
		Stream 1 (21 min ago)
			Running query32.sql (2 min ago)
				32 minutes, 500 MB/sec

run_test: 
updater.set_status(Running test 2 of 31, destroy_children=True)
	Sqlbatchtest.run:
		For each stream:
			Child_updater = updater.create_child(i)
			Child_Config.updater = child_updater
			Launch thread with child_updater
		Message loop:
			(no) When child finishes, updater.destroy_child(child_updater)
		Updater.set_status(done, x errors, x seconds, destroy_children=True)

Stream:
	Updater.set_status(running query 2)
	Run query
	Updater.set_status(query 2 done, x seconds, exception=error, show_in_recent_results=True)

