<?php 
	
	set_time_limit(100000);
	ini_set('memory_limit', '-1');
	error_reporting(E_ALL ^ E_NOTICE);
	
	if(function_exists($_GET['run'])) 
		$_GET['run']();


	function crawl() {

		echo "<h2>Crawler started<h2>";

		$name = 0;		
		$filename = 'json\decklist.json';
		$file = file_get_contents($filename);
		$json_a = json_decode($file, true);

		foreach ($json_a[results][Deck] as $key => $value) 
		{
			if ( isset($value[Event_ref][text]) && 
				$value[format][text] == "Modern") 
			{
				$url = $value[Download_link][href];

				echo "Format: " . $value[format][text] . "Json ID:" . $name . "<br/>";

				# If url is not proper, fix it	
				if (strpos($url,'deck/download') == false) {
		    		$url = str_replace('deck', 'deck/download', $url);
				}

				file_put_contents('crawled_lists\nb' . $name . '.txt', fopen($url, 'r'));

				$archetype = $value[Archetype][text];
		 		$file_content = file_get_contents('crawled_lists\nb' . $name . ".txt");

		  		file_put_contents('crawled__lists\nb'.$name.'.txt', $archetype . "\n" . $file_content);

				$name++;
			}
		}
	}	

	function stop() {
		echo "<h2>Crawled stoped</h2>";
		exit();
	}

?>