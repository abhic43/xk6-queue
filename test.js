import queue from 'k6/x/queue';
import { check, sleep } from 'k6';
import http from 'k6/http';

export let options = {
    scenarios: {
        // Order Creation Flow - Generates logisticOrderIds
        orderCreation: {
            executor: 'constant-arrival-rate',
            rate: 1, // 1 TPS
            timeUnit: '1s',
            duration: '60m',
            preAllocatedVUs: 2,
            maxVUs: 10,
            exec: 'createOrders'
        },
        
        // Fulfillment Flow 01 - Starts after 5 minutes delay
        fulfillmentFlow01: {
            executor: 'constant-arrival-rate',
            rate: 1, // 1 TPS  
            timeUnit: '1s',
            duration: '55m',
            startTime: '5m', // Start after 5 minutes
            preAllocatedVUs: 2,
            maxVUs: 10,
            exec: 'fulfillOrders01'
        },
        
        // Fulfillment Flow 02 - Starts after 20 minutes delay  
        fulfillmentFlow02: {
            executor: 'constant-arrival-rate',
            rate: 1, // 1 TPS
            timeUnit: '1s', 
            duration: '40m',
            startTime: '20m', // Start after 20 minutes
            preAllocatedVUs: 2,
            maxVUs: 10,
            exec: 'fulfillOrders02'
        }
    },
    
    thresholds: {
        http_req_duration: ['p(95)<2000'], // 95% of requests should be below 2s
        http_req_failed: ['rate<0.1'],     // Error rate should be below 10%
    }
};

// Order Creation Flow - Generates new logisticOrderIds
export function createOrders() {
    let logisticOrderId = `LO-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    
    // Simulate order creation API call
    let createOrderPayload = {
        orderId: logisticOrderId,
        customerId: `CUST-${Math.floor(Math.random() * 1000)}`,
        items: [
            {
                productId: `PROD-${Math.floor(Math.random() * 100)}`,
                quantity: Math.floor(Math.random() * 5) + 1
            }
        ]
    };
    
    // Replace with your actual API endpoint
    let response = http.post('https://httpbin.org/post', JSON.stringify(createOrderPayload), {
        headers: { 'Content-Type': 'application/json' },
        tags: { scenario: 'orderCreation' }
    });
    
    check(response, {
        'order creation status is 200': (r) => r.status === 200,
        'order creation response time < 2s': (r) => r.timings.duration < 2000,
    });
    
    if (response.status === 200) {
        // Push the generated order ID to the queue for consumption by fulfillment flows
        queue.push('logistic-orders', logisticOrderId);
        console.log(`✅ Created and queued order: ${logisticOrderId}`);
    } else {
        console.log(`❌ Failed to create order: ${logisticOrderId}`);
    }
    
    // Log queue statistics
    if (Math.random() < 0.1) { // Log stats 10% of the time to avoid spam
        console.log(`📊 Logistic orders queue size: ${queue.size('logistic-orders')}`);
    }
}

// Fulfillment Flow 01 - Consumes logisticOrderIds, generates distributionOrderIds and lastMileIds  
export function fulfillOrders01() {
    // Try to get an order with 10 second timeout
    let logisticOrderId = queue.popWithTimeout('logistic-orders', 10000);
    
    if (logisticOrderId === null) {
        console.log('⏳ No logistic orders available for fulfillment within timeout');
        return;
    }
    
    console.log(`🔄 Processing logistic order: ${logisticOrderId}`);
    
    // Generate fulfillment identifiers
    let distributionOrderId = `DO-${logisticOrderId.split('-')[1]}-${Math.random().toString(36).substr(2, 6)}`;
    let lastMileId = `LM-${logisticOrderId.split('-')[1]}-${Math.random().toString(36).substr(2, 6)}`;
    
    // Simulate fulfillment API calls
    let fulfillmentPayload = {
        logisticOrderId: logisticOrderId,
        distributionOrderId: distributionOrderId,
        lastMileId: lastMileId,
        fulfillmentCenter: `FC-${Math.floor(Math.random() * 10) + 1}`,
        estimatedDeliveryTime: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString()
    };
    
    // Replace with your actual fulfillment API endpoint
    let response = http.post('https://httpbin.org/post', JSON.stringify(fulfillmentPayload), {
        headers: { 'Content-Type': 'application/json' },
        tags: { scenario: 'fulfillmentFlow01' }
    });
    
    check(response, {
        'fulfillment 01 status is 200': (r) => r.status === 200,
        'fulfillment 01 response time < 2s': (r) => r.timings.duration < 2000,
    });
    
    if (response.status === 200) {
        // Push data for the next flow (Flow 02)
        let flow02Data = {
            distributionOrderId: distributionOrderId,
            lastMileId: lastMileId,
            originalLogisticOrderId: logisticOrderId,
            fulfillmentCenter: fulfillmentPayload.fulfillmentCenter,
            timestamp: Date.now()
        };
        
        queue.push('distribution-orders', flow02Data);
        console.log(`✅ Fulfilled order ${logisticOrderId} -> queued for final mile: ${distributionOrderId}`);
    } else {
        console.log(`❌ Failed to fulfill order: ${logisticOrderId}`);
        // Put the order back in the queue for retry (optional)
        queue.push('logistic-orders', logisticOrderId);
    }
    
    // Log queue statistics occasionally
    if (Math.random() < 0.1) {
        console.log(`📊 Distribution orders queue size: ${queue.size('distribution-orders')}`);
    }
}

// Fulfillment Flow 02 Alternate - Consumes data from Flow 01 (distributionOrderId, lastMileId)
export function fulfillOrders02() {
    // Try to get distribution order data with 10 second timeout
    let orderData = queue.popWithTimeout('distribution-orders', 10000);
    
    if (orderData === null) {
        console.log('⏳ No distribution orders available for final processing within timeout');
        return;
    }
    
    console.log(`🚚 Processing final mile delivery: ${orderData.distributionOrderId}`);
    
    // Generate final delivery tracking info
    let trackingNumber = `TRK-${Date.now()}-${Math.random().toString(36).substr(2, 8)}`;
    let deliveryAgentId = `DA-${Math.floor(Math.random() * 100) + 1}`;
    
    // Simulate final mile API calls
    let finalMilePayload = {
        distributionOrderId: orderData.distributionOrderId,
        lastMileId: orderData.lastMileId,
        originalLogisticOrderId: orderData.originalLogisticOrderId,
        trackingNumber: trackingNumber,
        deliveryAgentId: deliveryAgentId,
        fulfillmentCenter: orderData.fulfillmentCenter,
        deliveryStatus: 'OUT_FOR_DELIVERY',
        estimatedDeliveryTime: new Date(Date.now() + 4 * 60 * 60 * 1000).toISOString()
    };
    
    // Replace with your actual final mile API endpoint
    let response = http.post('https://httpbin.org/post', JSON.stringify(finalMilePayload), {
        headers: { 'Content-Type': 'application/json' },
        tags: { scenario: 'fulfillmentFlow02' }
    });
    
    check(response, {
        'fulfillment 02 status is 200': (r) => r.status === 200,
        'fulfillment 02 response time < 2s': (r) => r.timings.duration < 2000,
    });
    
    if (response.status === 200) {
        console.log(`✅ Final mile processed: ${orderData.distributionOrderId} -> Tracking: ${trackingNumber}`);
    } else {
        console.log(`❌ Failed final mile processing: ${orderData.distributionOrderId}`);
        // Put the order back in the queue for retry (optional)
        queue.push('distribution-orders', orderData);
    }
    
    // Log comprehensive statistics occasionally
    if (Math.random() < 0.05) { // 5% of the time
        let allQueues = queue.listQueues();
        console.log(`📊 Queue Status - Available queues: ${allQueues.join(', ')}`);
        allQueues.forEach(queueName => {
            console.log(`📊 ${queueName}: ${queue.size(queueName)} items`);
        });
    }
}

// Optional: Setup function that runs once before all scenarios
export function setup() {
    console.log('🚀 Starting load test with queue-based data flow...');
    console.log('📋 Test plan:');
    console.log('   - Order Creation: Starts immediately, runs for 60m');
    console.log('   - Fulfillment Flow 01: Starts at 5m, runs for 55m'); 
    console.log('   - Fulfillment Flow 02: Starts at 20m, runs for 40m');
    
    // Clear any existing queues (optional)
    let existingQueues = queue.listQueues();
    existingQueues.forEach(queueName => {
        queue.clear(queueName);
        console.log(`🧹 Cleared queue: ${queueName}`);
    });
    
    return { startTime: Date.now() };
}

// Optional: Teardown function that runs once after all scenarios complete
export function teardown(data) {
    console.log('🏁 Load test completed!');
    console.log(`⏱️  Total test duration: ${(Date.now() - data.startTime) / 1000}s`);
    
    // Final queue statistics
    let allQueues = queue.listQueues();
    console.log('📊 Final Queue Statistics:');
    allQueues.forEach(queueName => {
        let size = queue.size(queueName);
        console.log(`   ${queueName}: ${size} remaining items`);
        
        // Optionally, log some remaining items for debugging
        if (size > 0) {
            console.log(`   Sample remaining items in ${queueName}:`);
            for (let i = 0; i < Math.min(5, size); i++) {
                let item = queue.pop(queueName);
                console.log(`     - ${JSON.stringify(item)}`);
            }
        }
    });
}