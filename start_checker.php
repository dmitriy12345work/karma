<?php
error_reporting(E_ALL);

include_once(__DIR__ . '/config.php');
include_once(__DIR__ . '/utils.php');
$dbh = getDatabaseHandler($dbConfig);

emailsForSender($dbh, 1000); // добавляем уже подтвержденные в готовые к рассылке, чтоб письма можно было сразу рассылать, не дожидаясь проверки

// "garbage collector" - если произошла ошибка, или скрипт умер без Exception (убили процесс)
// то мы не проверенные email от таких процессов удаляем из таблицы emails, они будут добавлены заново.
// Если что-то проверилось - оставляем
runnerGarbageCollector($dbh, KK_RUNNER_CHECKER);

$runnersCount = getNumberOfRunners($dbh, KK_RUNNER_CHECKER);
printS("Runners count: $runnersCount. Runners limit: {$mailingConfig['checkersLimit']}");

$runnerId = 0;
// если можно еще запустить один поток проверок - запускаем
if ($runnersCount < $mailingConfig['checkersLimit']) {
    $runnerId = createRunner($dbh, KK_RUNNER_CHECKER);

    printS("Runner checker started: $runnerId");
    getEmailsForChecker($dbh, $runnerId, $mailingConfig);
}

printFinish($runnerId);
exit(0);

//////////////////////////////////////////////////
function getEmailsForChecker(\PDO $dbh, int $runnerId, array $mailingConfig): void
{
    $sql = 'INSERT INTO emails (email, checked, valid, runner_id)

            SELECT users.email, 0, confirmed, :runnerId
            FROM users
            LEFT JOIN emails em on users.email = em.email
            WHERE em.email IS NULL
            AND users.confirmed = 0 -- только то, что нужно проверять
            ORDER BY validts ASC    -- сначала те, что надо отправить раньше
            LIMIT ' . $mailingConfig['checkerAtOnce'];

    $sth = $dbh->prepare($sql, [PDO::ATTR_CURSOR => PDO::CURSOR_FWDONLY]);

    randomSleep();
    $dbh->beginTransaction();
    try {
        $sth->execute(['runnerId' => $runnerId]);
        $dbh->commit();
    } catch (\Throwable $exception) {
        $dbh->rollBack();
        printS(__METHOD__ . " Error {$exception->getCode()}: {$exception->getMessage()}");
        errorRunner($dbh, $runnerId);
    }

    try {
        $sql = 'SELECT *
                FROM emails
                WHERE runner_id = :runnerId';

        $sth = $dbh->prepare($sql, [PDO::ATTR_CURSOR => PDO::CURSOR_FWDONLY]);
        $sth->execute(['runnerId' => $runnerId]);

        $rows = $sth->fetchAll();

        foreach ($rows as $row) {
            processCheck($dbh, $row, $runnerId);
        }

        finishRunner($dbh, $runnerId);
    } catch (\Throwable $exception) {
        printS("Error {$exception->getCode()}: {$exception->getMessage()}");
        errorRunner($dbh, $runnerId);
    }
}

function processCheck(\PDO $dbh, array $row, int $runnerId): void
{
    $isValid = check_email($row['email']);

    $sql = 'UPDATE emails 
            SET checked = 1, valid = :isValid
            WHERE id = :rowId';
    $sth = $dbh->prepare($sql, [PDO::ATTR_CURSOR => PDO::CURSOR_FWDONLY]);

    $sth->execute([
        'isValid'   => (int)$isValid,
        'rowId'     => $row['id'],
    ]);

    updateRunner($dbh, $runnerId);
}

// Функция работает от 1 секунды до 1 минуты
function check_email(string $email): bool
{
    sleep(random_int(1, 60));

    // 50% что проверка будет успешной
    return random_int(1, 2) > 1;
}
