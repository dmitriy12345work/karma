<?php
error_reporting(E_ALL);

include_once(__DIR__ . '/config.php');
include_once(__DIR__ . '/utils.php');
$dbh = getDatabaseHandler($dbConfig);

// "garbage collector" - если произошла ошибка, или скрипт умер без Exception (убили процесс)
// то мы не проверенные email от таких процессов удаляем из таблицы emails, они будут добавлены заново.
// Если что-то проверилось - оставляем
runnerGarbageCollector($dbh, KK_RUNNER_SENDER);

$runnersCount = getNumberOfRunners($dbh, KK_RUNNER_SENDER);
printS("Runners count: $runnersCount. Runners limit: {$mailingConfig['sendersLimit']}");

$runnerId = 0;
// если можно еще запустить один поток проверок - запускаем
if ($runnersCount < $mailingConfig['checkersLimit']) {
    $runnerId = createRunner($dbh, KK_RUNNER_SENDER);

    printS("Runner sender started: $runnerId");
    getEmailsForSender($dbh, $runnerId, $mailingConfig);
}

printFinish($runnerId);
exit(0);

//////////////////////////////////////////////////
function getEmailsForSender(\PDO $dbh, int $runnerId, array $mailingConfig): void
{
    $sql = 'INSERT INTO emails_sent (email, sent_time, sent, runner_id)
                
            SELECT emails.email, NOW(), 0, :runnerId
            FROM emails
                     LEFT JOIN emails_sent es on emails.email = es.email
            WHERE es.email IS NULL
              AND emails.valid = 1
            ORDER BY emails.id ASC -- сначала те, что надо отправить раньше, их добавляли в начале
            LIMIT ' . $mailingConfig['sendAtOnce'];

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
                FROM emails_sent
                WHERE runner_id = :runnerId';

        $sth = $dbh->prepare($sql, [PDO::ATTR_CURSOR => PDO::CURSOR_FWDONLY]);
        $sth->execute(['runnerId' => $runnerId]);

        $rows = $sth->fetchAll();

        foreach ($rows as $row) {
            processSend($dbh, $row, $runnerId);
        }

        finishRunner($dbh, $runnerId);
    } catch (\Throwable $exception) {
        printS("Error {$exception->getCode()}: {$exception->getMessage()}");
        errorRunner($dbh, $runnerId);
    }
}

function processSend(\PDO $dbh, array $row, int $runnerId): void
{
    $isSent = send_email($row['email']);

    $sql = 'UPDATE emails_sent 
            SET sent = :isSent, sent_time = NOW()
            WHERE id = :rowId';
    $sth = $dbh->prepare($sql, [PDO::ATTR_CURSOR => PDO::CURSOR_FWDONLY]);

    $sth->execute([
        'isSent'    => (int)$isSent,
        'rowId'     => $row['id'],
    ]);

    updateRunner($dbh, $runnerId);
}

// Функция работает от 1 секунды до 10 секунд
function send_email(string $email, string $from = null, string $to = null, string $subj = null, string $body = null): bool
{
    sleep(random_int(1, 10));

    // считаем что будет успешной 100%, в задаче иное не указано
    return true;
}
