<?php

function getDatabaseHandler(array $dbConfig): \PDO
{
    return new PDO($dbConfig['dsn'], $dbConfig['user'], $dbConfig['password']);
}

// emails которые не надо проверять, т.к. подтверждены пользователем. Сразу пишем 1000 элементов на рассылку,
// чтоб не ждать проверку. Если 1.000.000 сразу отправить, то за 1 мин. до следующего запуска может не уложиться скрипт
function emailsForSender(\PDO $dbh, int $limit): void
{
    $sql = 'INSERT INTO emails (email, checked, valid, runner_id)
            -- runner_id = 0 потому что не требуется проверка
            SELECT users.email, 0, confirmed, 0
            FROM users
            LEFT JOIN emails em on users.email = em.email
            WHERE em.email IS NULL
            AND users.confirmed = 1 -- только то, что НЕ нужно проверять
            ORDER BY validts ASC    -- сначала те, что надо отправить раньше
            LIMIT ' . $limit;

    randomSleep();
    $dbh->beginTransaction();
    try {
        $dbh->exec($sql);
        $dbh->commit();
    } catch (\Throwable $exception) {
        $dbh->rollBack();
        printS(__METHOD__ . " Error {$exception->getCode()}: {$exception->getMessage()}");
    }
}

function createRunner(\PDO $dbh, int $runnerType): int
{
    $sql = 'INSERT INTO runners (type, start_time, last_time, error, finished) 
            VALUES (:runnerType, NOW(), NOW(), 0, 0)';

    $sth = $dbh->prepare($sql);
    $sth->execute(['runnerType' => $runnerType]);

    return $dbh->lastInsertId();
}

function errorRunner(\PDO $dbh, $runnerId): void
{
    $sql = 'UPDATE runners 
            SET last_time = NOW(), error = 1 
            WHERE id = :rowId';
    $sth = $dbh->prepare($sql);
    $sth->execute(['rowId'     => $runnerId]);
}

function finishRunner(\PDO $dbh, $runnerId): void
{
    $sql = 'UPDATE runners 
            SET finish_time = NOW(), finished = 1 
            WHERE id = :rowId';
    $sth = $dbh->prepare($sql);
    $sth->execute(['rowId'     => $runnerId]);
}

function finishWithErrorRunner(\PDO $dbh, $runnerId): void
{
    $sql = 'UPDATE runners 
            SET finish_time = NOW(), error = 1, finished = 1 
            WHERE id = :rowId';
    $sth = $dbh->prepare($sql);
    $sth->execute(['rowId'     => $runnerId]);
}

function getNumberOfRunners(\PDO $dbh, int $runnerType): int
{
    $runnersCount = 0;

    $sql = 'SELECT COUNT(*) AS runners_count 
            FROM runners 
            WHERE finished != 1
            AND error != 1
            AND type = :runnerType';

    $sth = $dbh->prepare($sql, [PDO::ATTR_CURSOR => PDO::CURSOR_FWDONLY]);
    $sth->execute(['runnerType' => $runnerType]);

    foreach ($sth->fetchAll() as $row) {
        $runnersCount = $row['runners_count'];
    }

    return $runnersCount;
}

// сохраняем информацию, что скрипт работает, записи обновляет
function updateRunner(\PDO $dbh, int $runnerId): void
{
    $sql = 'UPDATE runners SET last_time = NOW() WHERE id = :rowId';
    $sth = $dbh->prepare($sql);
    $sth->execute(['rowId'  => $runnerId]);
}

function runnerGarbageCollector(\PDO $dbh, int $runnerType): void
{
    $sql = 'SELECT * 
            FROM runners 
            WHERE finished != 1
            AND 
               (error = 1
            OR last_time <= DATE_SUB(NOW(), INTERVAL 5 MINUTE) -- по ТЗ, самый долгий скрипт может 1 минуту работать
                )
            AND type = :runnerType';

    $sth = $dbh->prepare($sql, [PDO::ATTR_CURSOR => PDO::CURSOR_FWDONLY]);
    $sth->execute(['runnerType' => $runnerType]);

    foreach ($sth->fetchAll() as $row) {
        match ($runnerType) {
            KK_RUNNER_CHECKER   => deleteErrorCheckedEmails($dbh, $row),
            KK_RUNNER_SENDER    => deleteErrorSenderEmails($dbh, $row),
            default => throw new \InvalidArgumentException("Unknown runner type: $runnerType"),
        };

        finishWithErrorRunner($dbh, $row['id']);
    }
}

function deleteErrorCheckedEmails(\PDO $dbh, array $runnerRow): void
{
    $sql = 'DELETE
            FROM emails 
            WHERE checked != 1
            AND runner_id = :runnerId';

    $sth = $dbh->prepare($sql);
    $sth->execute(['runnerId' => $runnerRow['id']]);
}

function deleteErrorSenderEmails(\PDO $dbh, array $runnerRow): void
{
    $sql = 'DELETE
            FROM emails_sent 
            WHERE sent != 1
            AND runner_id = :runnerId';

    $sth = $dbh->prepare($sql);
    $sth->execute(['runnerId' => $runnerRow['id']]);
}

function printFinish(int $runnerId): void
{
    printS("Finished successfully $runnerId at " . date('Y-m-d H:i:s'));
}

function printS(string $message): void
{
    print "\n $message \n";
}

// если запускаем параллельно много потоков, то что бы избегать Deadlock при больших вставках, делаем рандомную паузу
function randomSleep(): void
{
    usleep(random_int(10000, 1000000)); // чтобы не ровное число секунд было
    sleep(random_int(0, 3));
}
