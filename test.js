import queue from "k6/x/queue"; // Import your custom extension

export default function () {
  // Enqueue some items
  queue.enqueue("Message 1");
  queue.enqueue({ id: 1, data: "Hello" });
  queue.enqueue(42);

  // Check queue size
  console.log(`Current queue size: ${queue.size()}`); // Should be 3

  // Dequeue items
  let item1 = queue.dequeue();
  console.log(`Dequeued item: ${JSON.stringify(item1)}`);

  let item2 = queue.dequeue();
  console.log(`Dequeued item: ${JSON.stringify(item2)}`);

  console.log(`Current queue size: ${queue.size()}`); // Should be 1

  // Try to dequeue from an empty queue (will log an error from Go side)
  // Note: JavaScript will receive 'null' if the Go function returns nil and an error
  // or the error object itself depending on how it's handled.
  // For simplicity, our Go function returns nil and an error, which JS will see as null.
  queue.dequeue(); // This will log "queue is empty" from Go

  // Clear the queue
  queue.clear();
  console.log(`Current queue size after clear: ${queue.size()}`); // Should be 0
}
