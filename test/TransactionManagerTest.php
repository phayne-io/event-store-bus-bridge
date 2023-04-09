<?php

/**
 * This file is part of phayne-io/event-store-bus-bridge package.
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * @see       https://github.com/phayne-io/event-store-bus-bridge for the canonical source repository
 * @copyright Copyright (c) 2023 Phayne. (https://phayne.io)
 */

declare(strict_types=1);

namespace PhayneTest\EventStoreBusBridge;

use Phayne\EventStore\TransactionalEventStore;
use Phayne\EventStoreBusBridge\TransactionManager;
use Phayne\ServiceBus\CommandBus;
use Phayne\ServiceBus\Exception\MessageDispatchException;
use Phayne\ServiceBus\Plugin\Router\CommandRouter;
use PHPUnit\Framework\TestCase;
use Prophecy\PhpUnit\ProphecyTrait;
use RuntimeException;

/**
 * Class TransactionManagerTest
 *
 * @package PhayneTest\EventStoreBusBridge
 * @author Julien Guittard <julien@phayne.com>
 */
class TransactionManagerTest extends TestCase
{
    use ProphecyTrait;

    public function testHandlesTransactions(): void
    {
        $eventStore = $this->prophesize(TransactionalEventStore::class);

        $eventStore->beginTransaction()->shouldBeCalled();
        $eventStore->inTransaction()->willReturn(true)->shouldBeCalled();
        $eventStore->commit()->shouldBeCalled();

        $transactionManager = new TransactionManager($eventStore->reveal());

        $commandBus = new CommandBus();
        $router = new CommandRouter();
        $router->route('a message')->to(function () {
        });

        $router->attachToMessageBus($commandBus);

        $transactionManager->attachToMessageBus($commandBus);

        $commandBus->dispatch('a message');
    }

    public function testRollsBackTransactions(): void
    {
        $eventStore = $this->prophesize(TransactionalEventStore::class);

        $eventStore->beginTransaction()->shouldBeCalled();
        $eventStore->inTransaction()->willReturn(true)->shouldBeCalled();
        $eventStore->rollback()->shouldBeCalled();

        $transactionManager = new TransactionManager($eventStore->reveal());

        $commandBus = new CommandBus();
        $router = new CommandRouter();
        $router->route('a message')->to(function () {
            throw new RuntimeException('foo');
        });

        $router->attachToMessageBus($commandBus);

        $transactionManager->attachToMessageBus($commandBus);

        try {
            $commandBus->dispatch('a message');
        } catch (MessageDispatchException $e) {
            $this->assertInstanceOf(RuntimeException::class, $e->getPrevious());
            $this->assertEquals('foo', $e->getPrevious()->getMessage());

            return;
        }

        $this->fail('No exception thrown');
    }
}
