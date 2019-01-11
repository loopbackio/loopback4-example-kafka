import {
  KafkaClient,
  HighLevelProducer,
  ProduceRequest,
  ConsumerGroup,
} from 'kafka-node';

import {
  post,
  param,
  requestBody,
  get,
  RestBindings,
  Response,
  HttpErrors,
  del,
} from '@loopback/rest';
import {inject} from '@loopback/context';
import uuid = require('uuid');

//tslint:disable:no-any

const KAFKA_HOST = 'localhost:9092';

type FromOffset = 'earliest' | 'latest' | 'none';
interface SubscriptionRequest {
  topics: string[];
  groupId: string;
  fromOffset: FromOffset;
}

const consumers: {[id: string]: ConsumerGroup} = {};

export class KafkaDemoController {
  private client: KafkaClient;
  private producer: HighLevelProducer;

  constructor(
    @inject('kafka.host', {optional: true})
    private kafkaHost: string = KAFKA_HOST,
  ) {
    this.client = new KafkaClient({kafkaHost});
    this.producer = new HighLevelProducer(this.client, {});
  }

  /**
   * Wait for the producer to be ready
   */
  private isProducerReady() {
    return new Promise<void>((resolve, reject) => {
      this.producer.on('ready', () => resolve());
      this.producer.on('error', err => reject(err));
    });
  }

  private createConsumer(
    groupId: string,
    topics: string[],
    fromOffset: 'earliest' | 'latest' | 'none' = 'latest',
    clientId = uuid.v4(),
  ) {
    const consumer = new ConsumerGroup(
      {
        kafkaHost: this.kafkaHost,
        groupId: groupId || 'KafkaDemoController',
        fromOffset,
        id: clientId,
      },
      topics,
    );
    consumers[clientId] = consumer;
    return consumer;
  }

  private getConsumer(clientId: string) {
    return consumers[clientId];
  }

  private writeMessageToResponse(
    consumer: ConsumerGroup,
    limit: number,
    response: Response,
  ) {
    let count = 0;
    consumer.on('message', message => {
      count++;
      response.write(`id: ${message.offset}\n`);
      response.write('event: message\n');
      response.write(`data: ${JSON.stringify(message)}\n`);
      if (count >= limit) {
        response.end();
      }
      consumer.close(err => {
        if (err)
          console.log(
            'Something is wrong when closing the consumer.',
            err.message,
          );
      });
    });
    return response;
  }

  /**
   *
   * @param topic
   */
  @get('/consumers/{clientId}/messages', {
    responses: {
      '200': {
        'text/event-stream': {
          schema: {
            type: 'string',
          },
        },
      },
    },
  })
  async fetch(
    @param.path.string('clientId') clientId: string,
    @param.query.string('limit') limit: number,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ) {
    let consumer: ConsumerGroup | undefined = undefined;
    if (clientId) {
      consumer = this.getConsumer(clientId);
    }
    if (!consumer) {
      throw new HttpErrors.NotFound(`Consumer ${clientId} does not exist`);
    }
    limit = +limit || 5;
    response.setHeader('Cache-Control', 'no-cache');
    response.contentType('text/event-stream');

    this.writeMessageToResponse(consumer, limit, response);
    return response;
  }

  /**
   *
   * @param topic
   */
  @post('/consumers', {
    responses: {
      '200': {
        'application/json': {
          schema: {
            type: 'object',
            properties: {
              clientId: {type: 'string'},
            },
          },
        },
      },
    },
  })
  subscribe(
    @requestBody({
      content: {
        'application/json': {
          schema: {
            type: 'object',
            properties: {
              topics: {type: 'array', items: {type: 'string'}},
              fromOffset: {type: 'string'},
              groupId: {type: 'string'},
            },
          },
        },
      },
    })
    body: SubscriptionRequest,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ) {
    const consumer = this.createConsumer(
      body.groupId || 'KafkaDemoController',
      body.topics,
      body.fromOffset,
    );
    const client = consumer.client as any;
    return {
      clientId: client.clientId,
    };
  }

  @del('/consumers/{clientId}')
  async deleteConsumer(@param.path.string('clientId') clientId: string) {
    const consumer = this.getConsumer(clientId);
    if (consumer) {
      delete consumers[clientId];
      return new Promise<void>((resolve, reject) => {
        consumer.close(false, err => {
          if (err) reject(err);
          else resolve();
        });
      });
    } else {
      throw new HttpErrors.NotFound(`Consumer ${clientId} does not exist`);
    }
  }

  /**
   * Create topics
   * @param topics
   */
  @post('/topics')
  async createTopics(
    @requestBody({
      content: {
        'application/json': {
          schema: {
            type: 'array',
            items: {type: 'string'},
          },
        },
      },
    })
    topics: string[],
  ) {
    await this.isProducerReady();
    return new Promise<any>((resolve, reject) => {
      this.producer.createTopics(topics, true, (err, data) => {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }

  /**
   * Publish a message to the given topic
   * @param topic The topic name
   * @param message The message
   */
  @post('/topics/{topic}/messages')
  async publish(
    @param.path.string('topic') topic: string,
    @requestBody({
      content: {
        'application/json': {
          schema: {
            type: 'array',
            items: {type: 'string'},
          },
        },
      },
    })
    messages: string[],
  ) {
    await this.isProducerReady();
    const req: ProduceRequest = {topic, messages};
    return new Promise<any>((resolve, reject) => {
      this.producer.send([req], (err, data) => {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }

  /**
   * Consume messages of a given topic
   * @param topic The topic name
   */
  @get('/topics/{topic}/messages', {
    responses: {
      '200': {
        'text/event-stream': {
          schema: {
            type: 'string',
          },
        },
      },
    },
  })
  async consumeMessagesOnTopics(
    @param.path.string('topic') topic: string,
    @param.query.string('limit') limit: number,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ) {
    let consumer = this.createConsumer('', [topic], 'none');

    limit = +limit || 5;
    response.setHeader('Cache-Control', 'no-cache');
    response.contentType('text/event-stream');

    return this.writeMessageToResponse(consumer, limit, response);
  }
}
