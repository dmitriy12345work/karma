<?php

const KK_RUNNER_CHECKER     = 11022;
const KK_RUNNER_SENDER      = 11033;

$dbConfig = [
    'dsn'       => 'mysql:dbname=karma_db;host=127.0.0.1',
    'user'      => 'karma_user',
    'password'  => 'KARMA8password',
];

$mailingConfig = [
    'checkersLimit' => 100, // максимальное количество одновременно работающих скриптов
    'checkerAtOnce' => 15,  // делаем проверок за один запуск скрипта
    'sendersLimit'  => 100, // максимальное количество одновременно работающих скриптов
    'sendAtOnce'    => 100, // посылаем писем за один запуск скрипта
];
