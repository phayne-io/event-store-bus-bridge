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
use Phayne\EventStore\ActionEventEmitterEventStore;
use Phayne\EventStore\EventStore;
use Phayne\EventStore\Exception\StreamExistsAlready;
use Phayne\EventStore\InMemoryEventStore;
use Phayne\EventStore\Stream;
use Phayne\EventStore\StreamName;
use Phayne\EventStore\TransactionalActionEventEmitterEventStore;
use Phayne\EventStoreBusBridge\EventPublisher;
use Phayne\Messaging\Event\PhayneActionEventEmitter;
use Phayne\Messaging\Messaging\Message;
use Phayne\ServiceBus\EventBus;
use PHPUnit\Framework\TestCase;
use Prophecy\PhpUnit\ProphecyTrait;
use Throwable;

/**
 * Class EventPublisherTest
 *
 * @package PhayneTest\EventStoreBusBridge
 * @author Julien Guittard <julien@phayne.com>
 */
class EventPublisherTest extends TestCase
{
    use ProphecyTrait;

    private ActionEventEmitterEventStore $eventStore;

    protected function setUp(): void
    {
        $this->eventStore = new TransactionalActionEventEmitterEventStore(
            new InMemoryEventStore(),
            new PhayneActionEventEmitter()
        );
    }

    public function testPublishesAllCreatedAndAppendedEventsIfNotInsideTransaction(): void
    {
        [$event1, $event2, $event3, $event4] = $this->setupStubEvents();

        $eventBus = $this->prophesize(EventBus::class);

        $eventPublisher = new EventPublisher($eventBus->reveal());

        $eventPublisher->attachToEventStore($this->eventStore);

        $eventBus->dispatch($event1)->shouldBeCalled();
        $eventBus->dispatch($event2)->shouldBeCalled();
        $eventBus->dispatch($event3)->shouldBeCalled();
        $eventBus->dispatch($event4)->shouldBeCalled();

        $this->eventStore->create(new Stream(new StreamName('test'), new ArrayIterator([$event1, $event2])));
        $this->eventStore->appendTo(new StreamName('test'), new ArrayIterator([$event3, $event4]));
    }

    public function testPublishesAllCreatedAndAppendedEvents(): void
    {
        [$event1, $event2, $event3, $event4] = $this->setupStubEvents();

        $eventBus = $this->prophesize(EventBus::class);

        $eventBus->dispatch($event1)->shouldBeCalled();
        $eventBus->dispatch($event2)->shouldBeCalled();
        $eventBus->dispatch($event3)->shouldBeCalled();
        $eventBus->dispatch($event4)->shouldBeCalled();

        $eventPublisher = new EventPublisher($eventBus->reveal());

        $eventPublisher->attachToEventStore($this->eventStore);

        $this->eventStore->beginTransaction();
        $this->eventStore->create(new Stream(new StreamName('test'), new ArrayIterator([$event1, $event2])));
        $this->eventStore->appendTo(new StreamName('test'), new ArrayIterator([$event3, $event4]));
        $this->eventStore->commit();
    }

    public function itPublishesCorrectlyWhenEventStoreImplementsCanControlTransaction(): void
    {
        [$event1, $event2, $event3, $event4] = $this->setupStubEvents();

        $eventBus = $this->prophesize(EventBus::class);

        $eventBus->dispatch($event1)->shouldBeCalled();
        $eventBus->dispatch($event2)->shouldBeCalled();
        $eventBus->dispatch($event3)->shouldBeCalled();
        $eventBus->dispatch($event4)->shouldBeCalled();

        $eventPublisher = new EventPublisher($eventBus->reveal());

        $eventPublisher->attachToEventStore($this->eventStore);

        $this->eventStore->beginTransaction();
        $this->eventStore->create(new Stream(new StreamName('test'), new ArrayIterator([$event1, $event2])));
        $this->eventStore->appendTo(new StreamName('test'), new ArrayIterator([$event3, $event4]));
        $this->eventStore->commit();
    }

    public function testDoesNotPublishWhenEventStoreRollsBack(): void
    {
        [$event1, $event2, $event3, $event4] = $this->setupStubEvents();

        $eventBus = $this->prophesize(EventBus::class);

        $eventBus->dispatch($event1)->shouldNotBeCalled();
        $eventBus->dispatch($event2)->shouldNotBeCalled();
        $eventBus->dispatch($event3)->shouldNotBeCalled();
        $eventBus->dispatch($event4)->shouldNotBeCalled();

        $eventPublisher = new EventPublisher($eventBus->reveal());

        $eventPublisher->attachToEventStore($this->eventStore);

        $this->eventStore->beginTransaction();
        $this->eventStore->create(new Stream(new StreamName('test'), new ArrayIterator([$event1, $event2])));
        $this->eventStore->appendTo(new StreamName('test'), new ArrayIterator([$event3, $event4]));
        $this->eventStore->rollback();
    }

    public function itDoesNotPublishWhenNonTransactionalEventStoreThrowsException(): void
    {
        [$event1, $event2, $event3, $event4] = $this->setupStubEvents();

        $eventStore = $this->prophesize(EventStore::class);
        $eventStore->create(new Stream(new StreamName('test'), new ArrayIterator([$event1, $event2])))
            ->willThrow(StreamExistsAlready::with(new StreamName('test')))->shouldBeCalled();
        $eventStore->appendTo(new StreamName('test'), new ArrayIterator([$event3, $event4]))
            ->willThrow(new ConcurrencyException())->shouldBeCalled();
        $eventStore->appendTo(new StreamName('unknown'), new ArrayIterator([$event3, $event4]))
            ->willThrow(StreamNotFound::with(new StreamName('unknown')))->shouldBeCalled();

        $eventStore = new ActionEventEmitterEventStore($eventStore->reveal(), new PhayneActionEventEmitter());

        $eventBus = $this->prophesize(EventBus::class);

        $eventBus->dispatch($event1)->shouldNotBeCalled();
        $eventBus->dispatch($event2)->shouldNotBeCalled();
        $eventBus->dispatch($event3)->shouldNotBeCalled();
        $eventBus->dispatch($event4)->shouldNotBeCalled();

        $eventPublisher = new EventPublisher($eventBus->reveal());

        $eventPublisher->attachToEventStore($eventStore);

        try {
            $eventStore->create(new Stream(new StreamName('test'), new ArrayIterator([$event1, $event2])));
        } catch (Throwable) {
        }

        try {
            $eventStore->appendTo(new StreamName('test'), new ArrayIterator([$event3, $event4]));
        } catch (Throwable) {
        }

        try {
            $eventStore->appendTo(new StreamName('unknown'), new \ArrayIterator([$event3, $event4]));
        } catch (Throwable) {
        }
    }

    /**
     * @return Message[]
     */
    private function setupStubEvents(): array
    {
        $event1 = $this->prophesize(Message::class)->reveal();
        $event2 = $this->prophesize(Message::class)->reveal();
        $event3 = $this->prophesize(Message::class)->reveal();
        $event4 = $this->prophesize(Message::class)->reveal();

        return [$event1, $event2, $event3, $event4];
    }
}
