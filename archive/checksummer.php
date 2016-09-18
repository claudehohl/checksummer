<?php

//script must be run as root
if(! stristr(shell_exec("id"), 'root')){
	echo "must be run as root\n";
	exit;
}

//mysql connect
mysql_connect('localhost', 'checksummer', 'checksummer');
mysql_select_db('checksummer');
mysql_set_charset('utf8');
setlocale(LC_CTYPE, "UTF8", "de_CH.UTF-8");






if(! $argv[1]){
	echo "
	Usage:
	./$argv[0] [create_tables|check_existence] [rootpath]

	//initial
	Step 1: create_tables
	Step 2: collect_files
	Step 3: check_existence
	Step 4: check_filesize
	Step 5: make_checksum

	//checking
	Step 1: collect_files
	Step 2: check_existence
	Step 3: check_filesize
	Step 4: make_checksum
	Step 5: set_to_check
	Step 6: check_checksum

";
	exit;
}else{
	$argv[1]($argv[2]);
}








function create_tables(){

	//files
	mysql_query('CREATE TABLE IF NOT EXISTS `files` (
	`id` int(4) unsigned NOT NULL auto_increment,
	`rootpath` int(4) unsigned default NULL,
	`filename` text,
	`checksum_sha256` varchar(64) default NULL,
	`filesize` bigint(8) unsigned default NULL,
	`file_found` tinyint(3) unsigned default NULL,
	`checksum_ok` tinyint(3) unsigned default NULL,
	PRIMARY KEY  (`id`),
	KEY `filename` (`filename`(64)),
	KEY `checksum_sha256` (`checksum_sha256`),
	KEY `filesize` (`filesize`),
	KEY `file_found` (`file_found`),
	KEY `checksum_ok` (`checksum_ok`),
	FULLTEXT KEY `filename_2` (`filename`)
	) ENGINE=MyISAM  DEFAULT CHARSET=utf8 ;');

	//rootpaths
	mysql_query('CREATE TABLE IF NOT EXISTS `rootpaths` (
	`id` int(4) unsigned NOT NULL AUTO_INCREMENT,
	`rootpath` varchar(255) NOT NULL,
	PRIMARY KEY (`id`)
	) ENGINE=MyISAM  DEFAULT CHARSET=utf8 ;');

}



function collect_files($rootpath){

	if(! $rootpath){
		echo "Please give root path\n";
		exit;
	}

	$qrp = mysql_query("SELECT id FROM rootpaths WHERE rootpath = '" . $rootpath . "'");
	$rp = mysql_fetch_array($qrp);
	$rootpathid = $rp['id'];

	if($rootpathid == ''){
		mysql_query("INSERT INTO rootpaths (rootpath) VALUES ('" . $rootpath . "')");
		$rootpathid = mysql_insert_id();
	}

	echo "pushing filelist into array...\n";
	$filelist = shell_exec("find $rootpath -type f");
	$files = explode("\n", $filelist);

	$i = count($files);
	foreach($files as $file){

		if($file != ''){

			$file = str_replace($rootpath, '', $file);
			$file = str_replace("'", "\'", $file);
			$file = str_replace('"', '\"', $file);
			$q = mysql_query("SELECT id FROM files WHERE filename = '" . $file . "'");
			$f = mysql_fetch_array($q);

			if(count($f) > 1){ /* do nothing */ }else{
				echo "\nNew file: $file\n";
				mysql_query("INSERT INTO files (rootpath, filename) VALUES ('" . $rootpathid . "', '" . $file . "')");
			}

		}

		echo "$i\r";
		$i--;

	}

	echo "done.\n";

}



function check_existence($rootpath){

	if(! $rootpath){
		echo "Please give root path\n";
		exit;
	}

	$qrp = mysql_query("SELECT id FROM rootpaths WHERE rootpath = '" . $rootpath . "'");
	$rp = mysql_fetch_array($qrp);
	$rootpathid = $rp['id'];

	if($rootpathid == ''){
		echo "Rootpath not found\n";
		exit;
	}

	echo "Checking existence\n";
	$q = mysql_query("SELECT id, filename FROM files WHERE rootpath = '" . $rootpathid . "' AND file_found IS NULL");

	$i = mysql_affected_rows();
	while($file = mysql_fetch_array($q)){

		$filename = $rootpath . $file['filename'];

		echo '(' . $i . ') checking existence: ' . $filename . "\r";

		if(file_exists($filename)){
			mysql_query("UPDATE files SET file_found = '1' WHERE id = '" . $file['id'] . "'");
		}else{
			mysql_query("UPDATE files SET file_found = '0' WHERE id = '" . $file['id'] . "'");
		}

		$i--;

	}

	echo "\nDone\n";

}



function check_filesize($rootpath){

	if(! $rootpath){
		echo "Please give root path\n";
		exit;
	}

	$qrp = mysql_query("SELECT id FROM rootpaths WHERE rootpath = '" . $rootpath . "'");
	$rp = mysql_fetch_array($qrp);
	$rootpathid = $rp['id'];

	if($rootpathid == ''){
		echo "Rootpath not found\n";
		exit;
	}

	echo "Checking filesize\n";
	$q = mysql_query("SELECT id, filename, filesize FROM files WHERE rootpath = '" . $rootpathid . "' AND filesize IS NULL AND file_found = '1'");

	$i = mysql_affected_rows();
	while($file = mysql_fetch_array($q)){


		$filename = $rootpath . $file['filename'];

		echo '(' . $i . ') checking filesize ' . $filename . "\n";

		$filesize = shell_exec('sudo ls -l ' . escapeshellarg($filename) . " | awk '{ print $5 }'");

		if($filesize){
			if($file['filesize'] != ''){
				//not implemented: filesize mismatch
				//if(trim($file['filesize']) != trim($filesize)){
					//mysql_query("UPDATE files SET status = 'fsm' WHERE id = '" . $file['id'] . "'");
				//}
			}else{
				mysql_query("UPDATE files SET filesize = '" . $filesize . "' WHERE id = '" . $file['id'] . "'");
			}
		}

		$i--;

	}

}



//checksum
function check_checksum($rootpath){

	if(! $rootpath){
		echo "Please give root path\n";
		exit;
	}

	$qrp = mysql_query("SELECT id FROM rootpaths WHERE rootpath = '" . $rootpath . "'");
	$rp = mysql_fetch_array($qrp);
	$rootpathid = $rp['id'];

	if($rootpathid == ''){
		echo "Rootpath not found\n";
		exit;
	}

	echo "Checking checksum\n";
	$q = mysql_query("SELECT id, filename, filesize, checksum_sha256 FROM files WHERE rootpath = '" . $rootpathid . "' AND checksum_ok IS NULL");

	$i = mysql_affected_rows();
	while($file = mysql_fetch_array($q)){

		$filename = $rootpath . $file['filename'];

		echo '(' . $i . ') checking checksum: ' . $filename . "\n";

		if($file['filesize'] > (8 * 1024 * 1024)){
			$checksum = shell_exec('sha256sum ' . escapeshellarg($filename) . ' | awk \'{ print $1 }\'');
			$checksum = trim($checksum);
		}else{
			$checksum = hash_file('sha256', $filename);
		}

		if($checksum){
			if($checksum == $file['checksum_sha256']){
				mysql_query("UPDATE files SET checksum_ok = '1' WHERE id = '" . $file['id'] . "'");
			}else{
				echo $checksum . "\n" . $file['checksum_sha256'] . "\n\n";
				mysql_query("UPDATE files SET checksum_ok = '0' WHERE id = '" . $file['id'] . "'");
			}
		}else{
			mysql_query("UPDATE files SET file_found = '0' WHERE id = '" . $file['id'] . "'");
		}

		$i--;

	}

}



function make_checksum($rootpath){

	if(! $rootpath){
		echo "Please give root path\n";
		exit;
	}

	$qrp = mysql_query("SELECT id FROM rootpaths WHERE rootpath = '" . $rootpath . "'");
	$rp = mysql_fetch_array($qrp);
	$rootpathid = $rp['id'];

	if($rootpathid == ''){
		echo "Rootpath not found\n";
		exit;
	}

	echo "Making checksum\n";
	$q = mysql_query("SELECT id, filename, filesize FROM files WHERE rootpath = '" . $rootpathid . "' AND checksum_sha256 IS NULL AND file_found = '1'");
	
	$i = mysql_affected_rows();
	while($file = mysql_fetch_array($q)){
	
		$filename = $rootpath . $file['filename'];

		echo '(' . $i . ') making checksum: ' . $filename . "\n";

		if($file['filesize'] > (8 * 1024 * 1024)){
		       $checksum = shell_exec('sha256sum ' . escapeshellarg($filename) . ' | awk \'{ print $1 }\'');
		       $checksum = trim($checksum);
		}else{  
		       $checksum = hash_file('sha256', $filename);
		}

		if($checksum){
			mysql_query("UPDATE files SET checksum_sha256 = '" . $checksum . "' WHERE id = '" . $file['id'] . "'");
		}else{
			mysql_query("UPDATE files SET file_found = '0' WHERE id = '" . $file['id'] . "'");
		}

		$i--;

	}

}



function set_to_check(){

	echo "Setting status to check\n";
	mysql_query("UPDATE `checksummer`.`files` SET `checksum_ok` = NULL WHERE `file_found` = '1'");

}







