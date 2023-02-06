<?php
error_reporting(E_ALL);

include_once(__DIR__ . '/config.php');
include_once(__DIR__ . '/utils.php');
$dbh = getDatabaseHandler($dbConfig);

// Удаляем все тестовые данные. Тестовый скрипт, показывает как шла разработка

try {
    $sql = 'DROP TABLE IF EXISTS `users`';
    $dbh->exec($sql);

    $sql = 'DROP TABLE IF EXISTS `emails`';
    $dbh->exec($sql);

    $sql = 'DROP TABLE IF EXISTS `emails_sent`';
    $dbh->exec($sql);

    $sql = 'DROP TABLE IF EXISTS `runners`';
    $dbh->exec($sql);
} catch( \PDOException $e ) {
    printS("Error {$e->getCode()}: {$e->getMessage()}");
}
