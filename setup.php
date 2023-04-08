<?php

$projectName = basename(realpath("."));

// Reset README.md
file_put_contents( __DIR__ . '/README.md', "#$projectName");

// Edit composer.json
$composerFilepath = __DIR__ . '/composer.json';
$composerJson = file_get_contents($composerFilepath);
// Change name and description
$composerJson = preg_replace('/composer-template|Composer\spackage\stemplate/', $projectName, $composerJson);
// Use project name in Pascal case in psr-4 autoload
$projectName = preg_replace('/[-_]/', ' ', $projectName);
$projectName = ucwords($projectName);
$projectName = preg_replace('/\s/', '', $projectName);
$composerJson = preg_replace('/Template/', $projectName, $composerJson);
// Remove post-create-project-cmd script
$composerJson = preg_replace('/,\n.+"post-create-project-cmd":\s"php\ssetup\.php"/', '', $composerJson);
file_put_contents($composerFilepath, $composerJson);

$phpunitFilepath = __DIR__ . '/phpunit.xml.dist';
$phpunitXml = file_get_contents($phpunitFilepath);
$phpunitXml = preg_replace('/Template/',$projectName, $phpunitXml);
file_put_contents($phpunitFilepath, $phpunitXml);

// Remove template setup files
unlink(__DIR__ . '/setup.php');
rename(__DIR__ . '/gitattributes.tmpl', __DIR__ . '/.gitattributes');
