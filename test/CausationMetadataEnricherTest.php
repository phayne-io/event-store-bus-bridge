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

use ArrayIterator;
use Assert\InvalidArgumentException;
use Phayne\EventStore\ActionEventEmitterEventStore;
use Phayne\EventStore\EventStore;
use Phayne\EventStore\InMemoryEventStore;
use Phayne\EventStore\Stream;
use Phayne\EventStore\StreamName;
use Phayne\EventStoreBusBridge\CausationMetadataEnricher;
use Phayne\Messaging\Event\ActionEvent;
use Phayne\Messaging\Event\PhayneActionEventEmitter;
use Phayne\Messaging\Messaging\Message;
use Phayne\ServiceBus\CommandBus;
use Phayne\ServiceBus\MessageBus;
use Phayne\ServiceBus\Plugin\Router\CommandRouter;
use PhayneTest\ServiceBus\Mock\DoSomething;
use PhayneTest\ServiceBus\Mock\SomethingDone;
use PHPUnit\Framework\TestCase;
use Prophecy\PhpUnit\ProphecyTrait;

/**
 * Class CausationMetadataEnricherTest
 *
 * @package PhayneTest\EventStoreBusBridge
 * @author Julien Guittard <julien@phayne.com>
 */
class CausationMetadataEnricherTest extends TestCase
{
    use ProphecyTrait;

    public function testEnrichesCommandOnCreateStream(): void
    {
        $eventStore = $this->getEventStore();

        $causationMetadataEnricher = new CausationMetadataEnricher();

        $causationMetadataEnricher->attachToEventStore($eventStore);

        $commandBus = new CommandBus();
        $router = new CommandRouter();
        $router->route(DoSomething::class)->to(function (DoSomething $command) use ($eventStore): void {
            /* @var EventStore $eventStore */
            $eventStore->create(
                new Stream(
                    new StreamName('something'),
                    new ArrayIterator([
                        new SomethingDone(['name' => $command->payload('name')]),
                    ])
                )
            );
        });

        $result = null;

        $eventStore->attach(
            ActionEventEmitterEventStore::EVENT_CREATE,
            function (ActionEvent $event) use (&$result): void {
                $stream = $event->param('stream');
                $stream->streamEvents->rewind();
                $result = $stream->streamEvents->current();
            },
            -1000
        );

        $router->attachToMessageBus($commandBus);
        $causationMetadataEnricher->attachToMessageBus($commandBus);

        $command = new DoSomething(['name' => 'Alex'], 1);

        $commandBus->dispatch($command);

        $this->assertArrayHasKey('_causation_id', $result->metadata());
        $this->assertArrayHasKey('_causation_name', $result->metadata());

        $this->assertEquals($command->uuid()->toString(), $result->metadata()['_causation_id']);
        $this->assertEquals(get_class($command), $result->metadata()['_causation_name']);
    }

    public function testEnrichesCommandOnAppendToStream(): void
    {
        $eventStore = $this->getEventStore();

        $eventStore->create(
            new Stream(
                new StreamName('something'),
                new ArrayIterator()
            )
        );

        $causationMetadataEnricher = new CausationMetadataEnricher();

        $causationMetadataEnricher->attachToEventStore($eventStore);

        $commandBus = new CommandBus();
        $router = new CommandRouter();
        $router->route(DoSomething::class)->to(function (DoSomething $command) use ($eventStore): void {
            /* @var EventStore $eventStore */
            $eventStore->appendTo(
                new StreamName('something'),
                new ArrayIterator([
                    new SomethingDone(['name' => $command->payload('name')]),
                ])
            );
        });

        $result = null;

        $eventStore->attach(
            ActionEventEmitterEventStore::EVENT_APPEND_TO,
            function (ActionEvent $event) use (&$result): void {
                $streamEvents = $event->param('streamEvents');
                $streamEvents->rewind();
                $result = $streamEvents->current();
            },
            -1000
        );

        $router->attachToMessageBus($commandBus);
        $causationMetadataEnricher->attachToMessageBus($commandBus);

        $command = new DoSomething(['name' => 'John'], 1);

        $commandBus->dispatch($command);

        $this->assertArrayHasKey('_causation_id', $result->metadata());
        $this->assertArrayHasKey('_causation_name', $result->metadata());

        $this->assertEquals($command->uuid()->toString(), $result->metadata()['_causation_id']);
        $this->assertEquals(get_class($command), $result->metadata()['_causation_name']);
    }

    public function testReturnsEarlyIfCommandIsNullOnCreateStream(): void
    {
        $eventStore = $this->getEventStore();

        $causationMetadataEnricher = new CausationMetadataEnricher();

        $causationMetadataEnricher->attachToEventStore($eventStore);

        $commandBus = new CommandBus();
        $router = new CommandRouter();
        $router->route(DoSomething::class)->to(function (DoSomething $command) use ($eventStore): void {
            /* @var EventStore $eventStore */
            $eventStore->create(
                new Stream(
                    new StreamName('something'),
                    new ArrayIterator([
                        new SomethingDone(['name' => $command->payload('name')]),
                    ])
                )
            );
        });

        $result = null;

        $eventStore->attach(
            ActionEventEmitterEventStore::EVENT_CREATE,
            function (ActionEvent $event) use (&$result): void {
                $stream = $event->param('stream');
                $stream->streamEvents->rewind();
                $result = $stream->streamEvents->current();
            },
            -1000
        );

        $router->attachToMessageBus($commandBus);
        $causationMetadataEnricher->attachToMessageBus($commandBus);

        $command = new DoSomething(['name' => 'Alex'], 1);

        $commandBus->attach(
            MessageBus::EVENT_DISPATCH,
            function (ActionEvent $event): void {
                $event->setParam(MessageBus::EVENT_PARAM_MESSAGE, null);
            },
            MessageBus::PRIORITY_INVOKE_HANDLER + 2000
        );

        $commandBus->attach(
            MessageBus::EVENT_DISPATCH,
            function (ActionEvent $event) use ($command): void {
                $event->setParam(MessageBus::EVENT_PARAM_MESSAGE, $command);
            },
            MessageBus::PRIORITY_INVOKE_HANDLER + 500
        );

        $commandBus->dispatch($command);

        $this->assertArrayNotHasKey('_causation_id', $result->metadata());
        $this->assertArrayNotHasKey('_causation_name', $result->metadata());
    }

    public function testDetachesFromCommandBusAndEventStore(): void
    {
        $eventStore = $this->getEventStore();

        $causationMetadataEnricher = new CausationMetadataEnricher();

        $causationMetadataEnricher->attachToEventStore($eventStore);

        $commandBus = new CommandBus();
        $router = new CommandRouter();
        $router->route(DoSomething::class)->to(function (DoSomething $command) use ($eventStore): void {
            /* @var EventStore $eventStore */
            $eventStore->create(
                new Stream(
                    new StreamName('something'),
                    new ArrayIterator([
                        new SomethingDone(['name' => $command->payload('name')]),
                    ])
                )
            );
        });

        $result = null;

        $eventStore->attach(
            ActionEventEmitterEventStore::EVENT_CREATE,
            function (ActionEvent $event) use (&$result): void {
                $stream = $event->param('stream');
                $stream->streamEvents->rewind();
                $result = $stream->streamEvents->current();
            },
            -1000
        );

        $router->attachToMessageBus($commandBus);
        $causationMetadataEnricher->attachToMessageBus($commandBus);

        $causationMetadataEnricher->detachFromEventStore($eventStore);
        $causationMetadataEnricher->detachFromMessageBus($commandBus);

        $command = new DoSomething(['name' => 'John']);

        $commandBus->dispatch($command);

        $this->assertArrayNotHasKey('_causation_id', $result->metadata());
        $this->assertArrayNotHasKey('_causation_name', $result->metadata());
    }

    public function testReturnsEarlyIfCommandIsNullOnAppendToStream(): void
    {
        $eventStore = $this->getEventStore();

        $eventStore->create(
            new Stream(
                new StreamName('something'),
                new ArrayIterator()
            )
        );

        $causationMetadataEnricher = new CausationMetadataEnricher();

        $causationMetadataEnricher->attachToEventStore($eventStore);

        $commandBus = new CommandBus();
        $router = new CommandRouter();
        $router->route(DoSomething::class)->to(function (DoSomething $command) use ($eventStore): void {
            /* @var EventStore $eventStore */
            $eventStore->appendTo(
                new StreamName('something'),
                new ArrayIterator([
                    new SomethingDone(['name' => $command->payload('name')]),
                ])
            );
        });

        $result = null;

        $eventStore->attach(
            ActionEventEmitterEventStore::EVENT_APPEND_TO,
            function (ActionEvent $event) use (&$result): void {
                $streamEvents = $event->param('streamEvents');
                $streamEvents->rewind();
                $result = $streamEvents->current();
            },
            -1000
        );

        $router->attachToMessageBus($commandBus);
        $causationMetadataEnricher->attachToMessageBus($commandBus);

        $command = new DoSomething(['name' => 'John'], 1);

        $commandBus->attach(
            MessageBus::EVENT_DISPATCH,
            function (ActionEvent $event): void {
                $event->setParam(MessageBus::EVENT_PARAM_MESSAGE, null);
            },
            MessageBus::PRIORITY_INVOKE_HANDLER + 2000
        );

        $commandBus->attach(
            MessageBus::EVENT_DISPATCH,
            function (ActionEvent $event) use ($command): void {
                $event->setParam(MessageBus::EVENT_PARAM_MESSAGE, $command);
            },
            MessageBus::PRIORITY_INVOKE_HANDLER + 500
        );

        $commandBus->dispatch($command);

        $this->assertArrayNotHasKey('_causation_id', $result->metadata());
        $this->assertArrayNotHasKey('_causation_name', $result->metadata());
    }

    public function testHasConfigurableMetadataKeys(): void
    {
        $eventStore = $this->getEventStore();

        $idKey = '$causationId';
        $nameKey = '$causationName';
        $causationMetadataEnricher = new CausationMetadataEnricher($idKey, $nameKey);
        $causationMetadataEnricher->attachToEventStore($eventStore);

        $commandBus = new CommandBus();
        $router = new CommandRouter();
        $router->route(DoSomething::class)->to(function (DoSomething $command) use ($eventStore): void {
            /* @var EventStore $eventStore */
            $eventStore->create(
                new Stream(
                    new StreamName('something'),
                    new ArrayIterator([
                        new SomethingDone(['name' => $command->payload('name')]),
                    ])
                )
            );
        });

        $result = null;

        $eventStore->attach(
            ActionEventEmitterEventStore::EVENT_CREATE,
            function (ActionEvent $event) use (&$result): void {
                $stream = $event->param('stream');
                $stream->streamEvents->rewind();
                $result = $stream->streamEvents->current();
            },
            -1000
        );

        $router->attachToMessageBus($commandBus);
        $causationMetadataEnricher->attachToMessageBus($commandBus);

        $command = new DoSomething(['name' => 'John'], 1);
        $commandBus->dispatch($command);

        $this->assertArrayHasKey($idKey, $result->metadata());
        $this->assertArrayHasKey($nameKey, $result->metadata());

        $this->assertEquals($command->uuid()->toString(), $result->metadata()[$idKey]);
        $this->assertEquals(get_class($command), $result->metadata()[$nameKey]);
    }

    public function testCausationIdKeyCannotBeEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new CausationMetadataEnricher('');
    }

    public function testCausationNameKeyCannotBeEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new CausationMetadataEnricher('$causationId', '');
    }

    private function getEventStore(): ActionEventEmitterEventStore
    {
        return new ActionEventEmitterEventStore(new InMemoryEventStore(), new PhayneActionEventEmitter());
    }
}
