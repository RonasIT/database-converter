<?php
/**
 * Created by PhpStorm.
 * User: roman
 * Date: 12.04.17
 * Time: 16:27
 */

namespace RonasIT\Support\DatabaseConverter;

use Doctrine\DBAL\Platforms\MySqlPlatform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Platforms\PostgreSqlPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Illuminate\Cache\Repository;
use Illuminate\Support\Facades\DB;

class DataPuller
{
    protected $connection;
    protected $cache;
    protected $platforms = [
        'pgsql' => PostgreSqlPlatform::class,
        'mysql' => MySqlPlatform::class,
        'oracle' => OraclePlatform::class,
        'sqlsrv' => SQLServerPlatform::class,
        'sqlite' => SqlitePlatform::class
    ];
    protected $needToConvertToSnakeCase = false;
    protected $onlyData = false;
    protected $onlySchema = false;
    protected $dictionary = [];
    protected $tables = [];
    protected $indices = [];
    protected $sampleSize = 1000;

    public function __construct()
    {
        $this->cache = app(Repository::class);
    }

    public function setConvertToSnakeCase() {
        $this->needToConvertToSnakeCase = true;

        return $this;
    }

    public function setOnlyData() {
        $this->onlyData = true;

        return $this;
    }

    public function setOnlySchema() {
        $this->onlySchema = true;

        return $this;
    }

    public function setTables($tables) {
        $this->tables = $tables;

        return $this;
    }

    public function setSampleSize($size) {
        $this->sampleSize = $size;

        return $this;
    }

    public function pull($connectionName) {
        $this->connection = DB::connection($connectionName);

        $this->tables = $this->getTables();

        if ($this->needToConvertToSnakeCase){
            $this->buildDictionary();
        }

        if (!$this->onlyData) {
            $this->pullSchema();
        }

        if (!$this->onlySchema) {
            $this->pullData();
        }

        if (!$this->onlyData) {
            $this->pullIndices();
        }
    }

    protected function pullSchema() {
        $queries = $this->getQueries();

        foreach ($queries as $query) {
            $query = $this->prepareQuery($query);

            if ($this->isIndex($query)) {
                $this->indices[] = $query;

                continue;
            }

            DB::statement($query);
        }
    }

    protected function pullIndices() {
        foreach ($this->indices as $query) {
            DB::statement($query);
        }
    }

    protected function pullData() {
        foreach ($this->tables as $table) {
            $this->pullTable($table);
        }
    }

    protected function getQueries() {
        $platform = $this->getPlatform();

        return $this->cache->remember('queries', 100000, function () use ($platform) {
            return $this->connection
                ->getDoctrineSchemaManager()
                ->createSchema()
                ->toSql($platform);
        });
    }

    protected function getPlatform() {
        $connection = config('database.default');
        $platform = config("database.connections.{$connection}.driver");

        return app($this->platforms[$platform]);
    }

    protected function prepareQuery($query) {
        if ($this->needToConvertToSnakeCase) {
            return $this->convertToSnakeCase($query);
        }

        return $query;
    }

    protected function convertToSnakeCase($query) {
        foreach ($this->dictionary as $data) {
            if (str_contains($query, $data['origin'])) {
                $replacement = array_merge(
                    [$data], $data['columns']
                );

                $this->sortDictionary(
                    $replacement,
                    'replace_to'
                );

                foreach ($replacement as $replace) {
                    $this->replace($query, $replace['origin'], $replace['replace_to']);
                }
            }
        }

        return $query;
    }

    protected function buildDictionary() {
        foreach ($this->tables as $table) {
            $tableName = $table->getName();
            $convertedTableName = snake_case($tableName);

            $this->dictionary[$convertedTableName] = [
                'origin' => $tableName,
                'replace_to' => $convertedTableName
            ];

            foreach ($table->getColumns() as $column) {
                $columnName = $column->getName();
                $replaceTo = snake_case($columnName);

                if ($this->isDuplicateColumnName($convertedTableName, $replaceTo)) {
                    $replaceTo .= '_1';
                }

                $this->dictionary[$convertedTableName]['columns'][$columnName] = [
                    'origin' => $columnName,
                    'replace_to' => $replaceTo
                ];
            }
        }

        $this->sortDictionary($this->dictionary);
    }

    protected function addToDictionary($item) {
        $name = $item->getName();

        $this->dictionary[] = [
            'original' => $name,
            'replace_at' => snake_case($name)
        ];
    }

    protected function getTables() {
        $tables = $this->cache->remember('tables', 10000, function () {
            return $this->connection
                ->getDoctrineSchemaManager()
                ->listTables();
        });

        if (!empty($this->tables)) {
            return array_filter($tables, function ($table) {
                return in_array($table->getName(), $this->tables);
            });
        }

        return $tables;
    }

    public function pullTable($table) {
        $primaryKey = $this->getPrimaryKey($table);

        $this->connection->table($table->getName())
            ->orderBy($primaryKey)
            ->chunk(
                $this->sampleSize,
                $this->getPullDataCallback($table)
            );
    }

    protected function getPrimaryKey($table) {
        $primaryKeyColumns = $table->getPrimaryKeyColumns();

        return head($primaryKeyColumns);
    }

    protected function getPullDataCallback($table) {
        $tableName = $this->getItemName($table);

        return function ($items) use ($tableName) {
            $items = array_map(function ($item) use ($tableName) {
                return $this->prepareItem($item, $tableName);
            }, $items->toArray());

            DB::table($tableName)->insert($items);
        };
    }

    protected function prepareItem($item, $table)
    {
        $item = array_associate(
            (array)$item,
            function ($value, $key) use ($table) {
                if ($this->needToConvertToSnakeCase) {
                    $key = array_get(
                        $this->dictionary,
                        "{$table}.columns.{$key}.replace_to"
                    );
                }

                return [
                    'key' => $key,
                    'value' => $value
                ];
            }
        );

        return array_except($item, ['', null]);
    }

    protected function getItemName($item) {
        $item = $item->getName();

        if ($this->needToConvertToSnakeCase) {
            return snake_case($item);
        }

        return $item;
    }

    private function isIndex($query) {
        return str_contains($query, ' index ') || str_contains($query, 'foreign');
    }

    private function uniqueByValue($array, $callback) {
        $data = [];

        foreach ($array as $value) {
            $result = $callback($value);

            $data[$result] = $value;
        }

        return $data;
    }

    private function isDuplicateColumnName($table, $replaceTo) {
        if (empty($this->dictionary[$table]['columns'])) {
            return false;
        }

        $columnNameList = array_get_list(
            $this->dictionary,
            "{$table}.columns.*.replace_to"
        );

        return in_array($replaceTo, $columnNameList);
    }

    private function replace(&$haystack, $original, $replace) {
        $original = strtolower($original);
        $replacedAt = strtolower($replace);
        $haystack = strtolower($haystack);

        $haystack = str_replace($original, $replacedAt, $haystack);
    }

    private function sortDictionary(&$dictionary, $primaryKey = 'replace_to') {
        usort($dictionary, function ($a, $b) {
            if (strlen($a['replace_to']) == strlen($b['replace_to'])) {
                return 0;
            }

            return (strlen($a['replace_to']) > strlen($b['replace_to'])) ? -1 : 1;
        });

        $dictionary = $this->uniqueByValue($dictionary, function($item) use ($primaryKey) {
            return $item[$primaryKey];
        });
    }
}