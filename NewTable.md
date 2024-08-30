## All ideas about new table will be collected here

* расширить окно таймстампов которые мы принмаем до суток плюс сколько-то

* Обязательно: поменять условия фильтров в matview clickhouse обоих кластеров

* удалить из продов лишние таблицы

* добавить prekey index в основной кластер и включить, перед этим сбросить prekey date всем метрикам, у которых он стоит а то будут баги.

* Было бы неплохо как-то иметь отдельный max_count_host чтобы не терялись пики

* Новые колонки в новой таблице (идеи)
  1) key16..key32
  2) r0..r3 (64 bit raw)
  3) any(description) для событий
  4) one (пишется единичка, суммируется, показывает сколько рядов было саггрегировано)
  5) host int32 (mapped) - сюда будет иногда писаться то, что метрика считает хостом
  6) index_type для объединения индексов в 1 таблицу
  7) prekey станет всегда 64-бит для поддержки raw колонок
  8) надо попробовать максы доставать из макс хост, чтобы убрать колонку макс (это имеет минусы, и не вышло)

* Для новых таблиц увеличить парты до 12 часов
  PARTITION BY toStartOfInterval(time, toIntervalHour(12))

* Для новых таблиц при переносы не переносить ряды с count <= 0, у нас такие затесались в базу

*

## Улучшения вокруг статсхауса в Монолите

StatlogsViewApi
updateStatlogsOverrides что за хрень
function main_keys_overflow_monitoring(): int {
ID_APITKO, ID_EARTEMENKO, ID_AACULOVICH, ID_YALEXANDROV
что за трэш statlogsRetentionEvent
public static function getKeysHints(string $cluster, string $statlog_name) { убрать?
namespace VK\Stats; что это за хреновина?
что за хрень? hClickHouse_statlogs_buildTop
убрать дефолт параметры у  statlogsValueEvent
_statlogsShouldWriteActLog - что за хрень
statlogsStringTop( оставляет только 2 ключа
Cleaner :: markStatViewed что за хрень
statlogsSaveCallActStat что за штука? Разобраться, похоже на вызовы АПИ
'meowdb' statlogsViewCountIntersectionsMeow убрать
statlogsViewGetQueryMeow убрать
class PostStatsClusters { - заслуживает посмотреть, что это
statlogsAddKeyColumn разобраться что это
statlogsXEventInternalDBDirect - убрать?
