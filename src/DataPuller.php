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

        if (!$this->onlyData) {
            $this->pullSchema();
        }

        if (!$this->onlySchema) {
            $this->pullData();
        }
    }

    protected function pullSchema() {
        $queries = $this->getQueries();

        foreach ($queries as $query) {
            DB::statement(
                $this->prepareQuery($query)
            );
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
        if (empty($this->dictionary)) {
            $this->buildDictionary();
        }

        foreach ($this->dictionary as $key => $value) {
            $query = str_replace($key, $value, $query);
        }

        return $query;
    }

    protected function buildDictionary() {
        foreach ($this->tables as $table) {
            $this->addToDictionary($table);

            $columnNames = array_map(function ($column) {
                $this->addToDictionary($column);

                return snake_case($column->getName());
            }, $table->getColumns());

            $duplicatedNames = array_duplicate($columnNames);

            foreach ($duplicatedNames as $duplicatedName) {
                $this->dictionary[$duplicatedName] = "{$duplicatedName}_1";
            }
        }
    }

    protected function addToDictionary($item) {
        $name = $item->getName();

        $this->dictionary[$name] = snake_case($name);
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
        $dataWithConvertedKeys = array_associate(
            (array)$item,
            function ($value, $key) {
                if ($this->needToConvertToSnakeCase) {
                    $key = snake_case($key);
                }

                return [
                    'key' => $key,
                    'value' => $value
                ];
            }
        );

        $destinationDBFields = DB::getSchemaBuilder()->getColumnListing($table);

        return array_only($dataWithConvertedKeys, $destinationDBFields);
    }

    protected function getItemName($item) {
        $item = $item->getName();

        if ($this->needToConvertToSnakeCase) {
            return snake_case($item);
        }

        return $item;
    }
}