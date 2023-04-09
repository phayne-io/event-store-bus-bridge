<?php
//phpcs:ignorefile

/**
 * This file is part of phayne-io/event-store-bus-bridge package.
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * @see       https://github.com/phayne-io/event-store-bus-bridge for the canonical source repository
 * @copyright Copyright (c) 2023 Phayne. (https://phayne.io)
 */

declare(strict_types=1);

namespace Phayne\EventStoreBusBridge\Container;

use Phayne\EventStoreBusBridge\EventPublisher;
use Phayne\Exception\InvalidArgumentException;
use Phayne\ServiceBus\EventBus;
use Psr\Container\ContainerInterface;

use function sprintf;

/**
 * Class EventPublisherFactory
 *
 * @package Phayne\EventStoreBusBridge\Container
 * @author Julien Guittard <julien@phayne.com>
 */
final readonly class EventPublisherFactory
{
    public static function __callStatic(string $name, array $arguments): EventPublisher
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(sprintf(
                'The first argument must be of type %s',
                ContainerInterface::class
            ));
        }

        return (new EventPublisherFactory($name))->__invoke($arguments[0]);
    }

    public function __construct(private string $eventBusServiceName = EventBus::class)
    {
    }

    public function __invoke(ContainerInterface $container): EventPublisher
    {
        return new EventPublisher(
            $container->get($this->eventBusServiceName)
        );
    }
}
