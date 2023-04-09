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

use Phayne\EventStore\TransactionalEventStore;
use Phayne\Messaging\Event\ActionEvent;
use Phayne\ServiceBus\MessageBus;
use Phayne\ServiceBus\Plugin\AbstractPlugin;

/**
 * Class TransactionManager
 *
 * @package Phayne\EventStoreBusBridge
 * @author Julien Guittard <julien@phayne.com>
 */
final class TransactionManager extends AbstractPlugin
{
    public function __construct(private readonly TransactionalEventStore $eventStore)
    {
    }

    public function attachToMessageBus(MessageBus $messageBus): void
    {
        $this->listenerHandlers[] = $messageBus->attach(
            MessageBus::EVENT_DISPATCH,
            function (ActionEvent $event): void {
                $this->eventStore->beginTransaction();
            },
            MessageBus::PRIORITY_INVOKE_HANDLER + 1000
        );

        $this->listenerHandlers[] = $messageBus->attach(
            MessageBus::EVENT_FINALIZE,
            function (ActionEvent $event): void {
                if ($this->eventStore->inTransaction()) {
                    if ($event->param(MessageBus::EVENT_PARAM_EXCEPTION)) {
                        $this->eventStore->rollback();
                    } else {
                        $this->eventStore->commit();
                    }
                }
            },
            1000
        );
    }
}
