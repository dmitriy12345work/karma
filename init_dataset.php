<?php
error_reporting(E_ALL);

include_once(__DIR__ . '/config.php');
include_once(__DIR__ . '/utils.php');
$dbh = getDatabaseHandler($dbConfig);

// Заполняем начальные данные

try {
    $sql = 'CREATE TABLE IF NOT EXISTS `users` ( 
        id INT UNSIGNED NOT NULL AUTO_INCREMENT, 
        username VARCHAR(191) NOT NULL, -- 191 знак, потому что так 4 байтная unicode строка полностью входит в индекс. иначе надо индексировать не полную строку  
        email VARCHAR(191) NOT NULL, 
        validts TIMESTAMP NOT NULL, 
        confirmed TINYINT UNSIGNED NOT NULL,
        UNIQUE UNQ_users_email (email), 
        INDEX IDX_users_validts (validts),
        PRIMARY KEY (id)
        ) ENGINE = InnoDB CHARSET=utf8mb4 COLLATE utf8mb4_0900_ai_ci';
    $dbh->exec($sql);

    $sql = 'CREATE TABLE IF NOT EXISTS `emails` ( 
        id INT UNSIGNED NOT NULL AUTO_INCREMENT, 
        email VARCHAR(191) NOT NULL, 
        checked TINYINT UNSIGNED NOT NULL,
        valid TINYINT UNSIGNED NOT NULL,
        runner_id INT UNSIGNED NOT NULL,
        UNIQUE UNQ_emails_email (email),
        INDEX IDX_emails_valid (valid),
        INDEX IDX_emails_runner_id (runner_id),
        PRIMARY KEY (id)
        ) ENGINE = InnoDB CHARSET=utf8mb4 COLLATE utf8mb4_0900_ai_ci';
    $dbh->exec($sql);

    $sql = 'CREATE TABLE IF NOT EXISTS `emails_sent` ( 
        id INT UNSIGNED NOT NULL AUTO_INCREMENT, 
        email VARCHAR(191) NOT NULL, 
        sent_time DATETIME NOT NULL, -- заполняем текущим временем, потом обновляем при отправке, так быстрее работает, чем null обновлять
        sent TINYINT UNSIGNED NOT NULL,
        runner_id INT UNSIGNED NOT NULL,
        UNIQUE UNQ_emails_sent_email (email),
        INDEX IDX_emails_sent_runner_id (runner_id),
        PRIMARY KEY (id)
        ) ENGINE = InnoDB CHARSET=utf8mb4 COLLATE utf8mb4_0900_ai_ci';
    $dbh->exec($sql);

    $sql = 'CREATE TABLE IF NOT EXISTS `runners` ( 
        id INT UNSIGNED NOT NULL AUTO_INCREMENT, 
        type SMALLINT UNSIGNED NOT NULL, -- 11022 - checker; 11033 - sender (unique id, easy to search)
        start_time DATETIME NOT NULL,
        last_time DATETIME NULL,
        finish_time DATETIME NULL,
        error TINYINT UNSIGNED NOT NULL,
        finished TINYINT UNSIGNED NOT NULL,
        INDEX IDX_runners_finished (finished),
        PRIMARY KEY (id)
        ) ENGINE = InnoDB CHARSET=utf8mb4 COLLATE utf8mb4_0900_ai_ci';
    $dbh->exec($sql);
} catch( \PDOException $e ) {
    printS("Error {$e->getCode()}: {$e->getMessage()}");
}

// заполняем рандомными данными таблицу пользователей
// 1.000.000 строк
for ($i = 0; $i < 100; $i++) {
    try {
        $dbh->exec(getUsersSql(10000));
    } catch( \PDOException $e ) {
        printS("Error {$e->getCode()}: {$e->getMessage()}");
    }

    if (0 === ($i % 10)) {
        printS(" $i " . date('Y-m-d H:i:s'));
    }
}

printFinish(0);

//////////////////////////////////////////////////
function getUsersSql(int $generateRows): string
{
    return 'INSERT INTO users
        (username, email, validts, confirmed) VALUES ' . getUserInserts($generateRows);
}

function getUserInserts(int $generateRows): string
{
    $rows = [];

    for ($i = 0; $i < $generateRows; $i++) {
        $rows[] = getUserInsertString();
    }

    return implode(", ", $rows);
}

function getUserInsertString(): string
{
    return vsprintf('("%s", "%s", "%s", %d)', getUserRandomData());
}

function getUserRandomData(): array
{
    $randomName = uniqid('', true);

    return [
        'username'  => $randomName,
        'email'     => $randomName . '@test.com',
        'validts'   => randomDate(-60, 360)->format('Y-m-d H:i:s'),
        'confirmed' => randomConfirm(80), // считаем, что большая часть людей подтвердит адрес
    ];
}

function randomConfirm(int $probability): bool
{
    // подтверждаем email с заданной вероятностью
    return random_int(1, 100) < $probability;
}

// генерим дату с разбросом в минутах от даты 3 дня в будущем
function randomDate(int $subtractMinutes, int $addMinutes): \DateTime
{
    $randMin = random_int($subtractMinutes, $addMinutes);

    return new \DateTime("3 days $randMin minutes");
}