<?php

namespace RonasIT\Support\DatabaseConverter\Commands;

use RonasIT\Support\DatabaseConverter\DataPuller;
use Illuminate\Console\Command;
use Illuminate\Config\Repository as Config;

/**
 * @property DataPuller $dataPuller
 * @property Config $config
*/
class DatabaseConvert extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'convert:db
        {--driver=}
        {--host=} 
        {--password=} 
        {--user=} 
        {--db=} 
        {--port=}
        {--charset=utf-8}
        {--prefix=}
        {--convert-to-snake-case}
        ';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = '
        Pull all schema and data from specified database to project database
        WARNING: this command require blank database specified in config
    ';

    protected $config;
    protected $dataPuller;

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();

        $this->dataPuller = app(DataPuller::class);
        $this->config = app(Config::class);
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $this->buildConnection();

        if ($this->option('convert-to-snake-case')) {
            $this->dataPuller->setConvertToSnakeCase();
        }

        $this->dataPuller->pull('source');
    }

    protected function buildConnection() {
        $this->config->set('database.connections.source', [
            'driver' => $this->option('driver'),
            'host' => $this->option('host'),
            'password' => $this->option('password'),
            'username' => $this->option('user'),
            'database' => $this->option('db'),
            'charset' => $this->option('charset'),
            'prefix' => $this->option('prefix')
        ]);
    }
}
