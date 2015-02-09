<?php
/*
*	Clean crawled files
* 	File stucture:

UR
// JohnnyHotSauce (3 - 1)
// Modern Daily #4150815 on 2012-08-03
3 Island
4 Scalding Tarn
4 Desperate Ritual
4 Grapeshot
3 Pyromancer Ascension
2 Empty the Warrens
4 Pyretic Ritual
4 Manamorphose
4 Gitaxian Probe
3 Past in Flames
4 Seething Song
1 Breeding Pool
1 Mountain
3 Sulfur Falls
4 Sleight of Hand
2 Steam Vents
2 Misty Rainforest
4 Serum Visions
4 Thought Scour

1 Lightning Bolt
3 Defense Grid
1 Empty the Warrens
1 Kiln Fiend
2 Pyroclasm
3 Ancient Grudge
1 Echoing Truth
3 Seal of Fire
------------------------------------------------------------------------------------
*	Produce :
JohnnyHotSauce	3,1	4150815	UR	3$$Island&&4$$Scalding Tarn&&4$$Desperate 
Ritual&&4$$Grapeshot&&3$$Pyromancer Ascension&&2$$Empty the Warrens&&4$$Pyretic 
Ritual&&4$$Manamorphose&&4$$Gitaxian Probe&&3$$Past in Flames&&4$$Seething 
Song&&1$$Breeding Pool&&1$$Mountain&&3$$Sulfur Falls&&4$$Sleight of Hand&&2$$Steam 
Vents&&2$$Misty Rainforest&&4$$Serum Visions&&4$$Thought Scour&&1$$Lightning 
Bolt&&3$$Defense Grid&&1$$Empty the Warrens&&1$$Kiln Fiend&&2$$Pyroclasm&&3$$Ancient 
Grudge&&1$$Echoing Truth&&3$$Seal of Fire
*/
$target = 'deck_150233-160233.txt';
$buffer = fopen($target, "w");

for ($i=0; $i <= 1096; $i++) { 

	$source = $i.'.txt';
	$handle = fopen("deck_150233-160233/".$source, "r");

	$counter = 0;
	$lines = array();

	if ($handle) 
	{
	    while (($line = fgets($handle)) !== false) 
	    {

	    	if ( $counter == 0 ) 
	    	{
	    		$lines[$counter] = $line;
	    	}
	    	elseif ( $counter == 1)
	    	{
	    		$line = preg_replace('/^.*\/\/\s*/', '', $line);
	    		$string = explode(' (', $line);
	    		$string[1] = str_replace(array(')' ), '', $string[1]);
	    		$string[1] = str_replace(array(' - '), ',', $string[1]);
	    		$line2 = $string[0] . "\t" . $string[1];
	    		$lines[$counter] = $line2;
	     		
	    	}
	    	elseif ( $counter == 2) 
	    	{
	    		$line = strstr($line, '#');
	    		$line = substr($line, 0, strrpos($line, 'o'));
	    		$line = rtrim($line);
	    		$line = preg_replace("/[^0-9]/","",$line);
	    		
	    		$lines[1] = $lines[1] . "\t" . $line . "\t" . $lines[0] . "\t"; 
	    	}
	    	elseif (empty(trim($line))) { continue; }
	    	elseif ( $counter == 3 )
	    	{
	    		$numbers = preg_replace("/[^0-9]/","",$line);
	    		$line = trim(preg_replace('/[^a-z., ]/i','',$line));   
	    		$lines[1] = $lines[1] . $numbers . "$$" .$line;
	    	}
	    	else 
	    	{
	    		$numbers = preg_replace("/[^0-9]/","",$line);
	    		$line = trim(preg_replace('/[^a-z., ]/i','',$line));   
	    		//$line = preg_replace('/(?<=\d)/', "$$", $line);
	    		$lines[1] = $lines[1] . "&&" .$numbers . "$$" .$line;
	    	}

	    	$counter++;
	    }
	    $string = str_replace(array("\r", "\n"), '', $lines[1]);
	    fwrite($buffer, $string . "\n");
	    fclose($handle);
	} 
	else 
	{
	    echo "Error opening file.";
	}
}
fclose($buffer);

?>