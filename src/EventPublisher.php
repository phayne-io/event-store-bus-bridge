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

namespace Phayne\EventStoreBusBridge;

use ArrayIterator;
use Phayne\EventStore\ActionEventEmitterEventStore;
use Phayne\EventStore\EventStore;
use Phayne\EventStore\Plugin\AbstractPlugin;
use Phayne\EventStore\Stream;
use Phayne\EventStore\TransactionalActionEventEmitterEventStore;
use Phayne\Messaging\Event\ActionEvent;
use Phayne\ServiceBus\EventBus;

/**
 * Class EventPublisher
 *
 * @package Phayne\EventStoreBusBridge
 * @author Julien Guittard <julien@phayne.com>
 */
final class EventPublisher extends AbstractPlugin
{
    private array $cachedEventStreams = [];

    public function __construct(private readonly EventBus $eventBus)
    {
    }

    public function attachToEventStore(ActionEventEmitterEventStore $eventStore): void
    {
        $this->listenerHandlers[] = $eventStore->attach(
            ActionEventEmitterEventStore::EVENT_APPEND_TO,
            function (ActionEvent $event) use ($eventStore): void {
                $recordedEvents = $event->param('streamEvents', new ArrayIterator());

                if (false === $this->inTransaction($eventStore)) {
                    if (
                        $event->param('streamNotFound', false) ||
                        $event->param('concurrencyException', false)
                    ) {
                        return;
                    }

                    foreach ($recordedEvents as $recordedEvent) {
                        $this->eventBus->dispatch($recordedEvent);
                    }
                } else {
                    $this->cachedEventStreams[] = $recordedEvents;
                }
            }
        );

        $this->listenerHandlers[] = $eventStore->attach(
            ActionEventEmitterEventStore::EVENT_CREATE,
            function (ActionEvent $event) use ($eventStore): void {
                /** @var Stream $stream */
                $stream = $event->param('stream');
                $recordedEvents = $stream->streamEvents;

                if (false === $this->inTransaction($eventStore)) {
                    if ($event->param('streamExistsAlready', false)) {
                        return;
                    }

                    foreach ($recordedEvents as $recordedEvent) {
                        $this->eventBus->dispatch($recordedEvent);
                    }
                } else {
                    $this->cachedEventStreams[] = $recordedEvents;
                }
            }
        );

        if ($eventStore instanceof TransactionalActionEventEmitterEventStore) {
            $this->listenerHandlers[] = $eventStore->attach(
                TransactionalActionEventEmitterEventStore::EVENT_COMMIT,
                function (ActionEvent $event): void {
                    foreach ($this->cachedEventStreams as $stream) {
                        foreach ($stream as $recordedEvent) {
                            $this->eventBus->dispatch($recordedEvent);
                        }
                    }
                    $this->cachedEventStreams = [];
                }
            );

            $this->listenerHandlers[] = $eventStore->attach(
                TransactionalActionEventEmitterEventStore::EVENT_ROLLBACK,
                function (ActionEvent $event): void {
                    $this->cachedEventStreams = [];
                }
            );
        }
    }

    private function inTransaction(EventStore $eventStore): bool
    {
        return $eventStore instanceof TransactionalActionEventEmitterEventStore
            && $eventStore->inTransaction();
    }
}
