<?php
/**
 * Created by PhpStorm.
 * User: roman
 * Date: 10.04.17
 * Time: 19:48
 */
namespace RonasIT\Support\DatabaseConverter;

use RonasIT\Support\DatabaseConverter\Commands\DatabaseConvert;
use Illuminate\Support\ServiceProvider;

class DatabaseConverterServiceProvider extends ServiceProvider
{
    public function boot() {
        $this->commands([
            DatabaseConvert::class
        ]);
    }
}