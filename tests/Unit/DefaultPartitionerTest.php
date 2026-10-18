<?php

namespace Siberfx\Samsa\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Siberfx\Samsa\Producer\DefaultPartitioner;

final class DefaultPartitionerTest extends TestCase
{
    public function test_the_same_key_always_maps_to_the_same_partition(): void
    {
        $partitioner = new DefaultPartitioner;

        $first = $partitioner->partition('user-42', 8);

        for ($i = 0; $i < 50; $i++) {
            $this->assertSame($first, $partitioner->partition('user-42', 8));
        }

        $this->assertSame($first, (new DefaultPartitioner)->partition('user-42', 8), 'stable across instances');
    }

    public function test_keys_are_spread_over_every_partition(): void
    {
        $partitioner = new DefaultPartitioner;
        $counts = array_fill(0, 8, 0);

        for ($i = 0; $i < 8000; $i++) {
            $counts[$partitioner->partition("order-{$i}", 8)]++;
        }

        foreach ($counts as $partition => $count) {
            $this->assertGreaterThan(800, $count, "partition {$partition} is underused");
            $this->assertLessThan(1200, $count, "partition {$partition} is overused");
        }
    }

    public function test_null_keys_are_round_robin(): void
    {
        $partitioner = new DefaultPartitioner;

        $this->assertSame([0, 1, 2, 0, 1], array_map(fn () => $partitioner->partition(null, 3), range(1, 5)));
    }

    public function test_results_are_always_in_range(): void
    {
        $partitioner = new DefaultPartitioner;

        foreach (['', 'a', str_repeat('x', 1000), "\u{1F600}"] as $key) {
            $partition = $partitioner->partition($key, 7);
            $this->assertGreaterThanOrEqual(0, $partition);
            $this->assertLessThan(7, $partition);
        }
    }
}
