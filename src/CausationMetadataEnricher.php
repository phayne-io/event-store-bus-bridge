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
use Assert\Assertion;
use Phayne\EventStore\ActionEventEmitterEventStore;
use Phayne\EventStore\Metadata\MetadataEnricher;
use Phayne\EventStore\Plugin\Plugin as EventStorePlugin;
use Phayne\EventStore\Stream;
use Phayne\Messaging\Event\ActionEvent;
use Phayne\Messaging\Messaging\Message;
use Phayne\ServiceBus\MessageBus;
use Phayne\ServiceBus\Plugin\Plugin as MessageBusPlugin;

/**
 * Class CausationMetadataEnricher
 *
 * @package Phayne\EventStoreBusBridge
 * @author Julien Guittard <julien@phayne.com>
 */
final class CausationMetadataEnricher implements MetadataEnricher, EventStorePlugin, MessageBusPlugin
{
    private ?Message $currentCommand = null;

    private array $eventStoreListeners = [];

    private array $messageBusListeners = [];

    public function __construct(
        private readonly string $causationIdKey = '_causation_id',
        private readonly string $causationNameKey = '_causation_name'
    ) {
        Assertion::notEmpty($this->causationIdKey);
        Assertion::notEmpty($this->causationNameKey);
    }

    public function enrich(Message $message): Message
    {
        $message = $message->withAddedMetadata($this->causationIdKey, $this->currentCommand?->uuid()->toString());
        return $message->withAddedMetadata($this->causationNameKey, $this->currentCommand->messageName());
    }

    public function attachToEventStore(ActionEventEmitterEventStore $eventStore): void
    {
        $this->eventStoreListeners[] = $eventStore->attach(
            ActionEventEmitterEventStore::EVENT_APPEND_TO,
            function (ActionEvent $event): void {
                if (! $this->currentCommand instanceof Message) {
                    return;
                }

                $recordedEvents = $event->param('streamEvents');
                $enrichedRecordedEvents = [];

                foreach ($recordedEvents as $recordedEvent) {
                    $enrichedRecordedEvents[] = $this->enrich($recordedEvent);
                }

                $event->setParam('streamEvents', new ArrayIterator($enrichedRecordedEvents));
            },
            1000
        );

        $this->eventStoreListeners[] = $eventStore->attach(
            ActionEventEmitterEventStore::EVENT_CREATE,
            function (ActionEvent $event): void {
                if (! $this->currentCommand instanceof Message) {
                    return;
                }

                /** @var Stream $stream */
                $stream = $event->param('stream');
                $recordedEvents = $stream->streamEvents;
                $enrichedRecordedEvents = [];

                foreach ($recordedEvents as $recordedEvent) {
                    $enrichedRecordedEvents[] = $this->enrich($recordedEvent);
                }

                $stream = new Stream(
                    $stream->streamName,
                    new ArrayIterator($enrichedRecordedEvents),
                    $stream->metadata
                );

                $event->setParam('stream', $stream);
            },
            1000
        );
    }

    public function detachFromEventStore(ActionEventEmitterEventStore $eventStore): void
    {
        foreach ($this->eventStoreListeners as $listenerHandler) {
            $eventStore->detach($listenerHandler);
        }

        $this->eventStoreListeners = [];
    }

    public function attachToMessageBus(MessageBus $messageBus): void
    {
        $this->messageBusListeners[] = $messageBus->attach(
            MessageBus::EVENT_DISPATCH,
            function (ActionEvent $event): void {
                $this->currentCommand = $event->param(MessageBus::EVENT_PARAM_MESSAGE);
            },
            MessageBus::PRIORITY_INVOKE_HANDLER + 1000
        );

        $this->messageBusListeners[] = $messageBus->attach(
            MessageBus::EVENT_FINALIZE,
            function (ActionEvent $event): void {
                $this->currentCommand = null;
            },
            1000
        );
    }

    public function detachFromMessageBus(MessageBus $messageBus): void
    {
        foreach ($this->messageBusListeners as $listenerHandler) {
            $messageBus->detach($listenerHandler);
        }

        $this->messageBusListeners = [];
    }
}
