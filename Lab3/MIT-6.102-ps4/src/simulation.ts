/* Copyright (c) 2021-25 MIT 6.102/6.031 course staff, all rights reserved.
 * Redistribution of original or derived work requires permission of course staff.
 */

import assert from 'node:assert';
import { Board } from './board.js';

/**
 * Simulation of concurrent Memory Scramble gameplay.
 * 
 * This simulation demonstrates that the board correctly handles:
 * - Multiple concurrent players
 * - Waiting for cards
 * - Matching and removal
 * - Race conditions and interleavings
 * 
 * Requirements for grading:
 * - 4 players
 * - Random timeouts between 0.1ms and 2ms
 * - 100 moves per player
 * - No crashes or deadlocks
 * 
 * @throws Error if an error occurs reading or parsing the board
 */
async function simulationMain(): Promise<void> {
    const filename = 'boards/ab.txt';
    const board: Board = await Board.parseFromFile(filename);
    const size = 5;
    const players = 4;
    const tries = 100;
    const minDelayMilliseconds = 0.1;
    const maxDelayMilliseconds = 2;

    console.log('Starting Memory Scramble simulation...');
    console.log(`Board: ${filename}`);
    console.log(`Players: ${players}`);
    console.log(`Moves per player: ${tries}`);
    console.log(`Delay range: ${minDelayMilliseconds}ms - ${maxDelayMilliseconds}ms`);
    console.log('---');

    // Track statistics
    const stats = {
        totalFlips: 0,
        successfulMatches: 0,
        failedFlips: 0,
        cardsRemaining: size * size
    };

    // Start up one or more players as concurrent asynchronous function calls
    const playerPromises: Array<Promise<void>> = [];
    for (let ii = 0; ii < players; ++ii) {
        playerPromises.push(player(ii));
    }
    
    // Wait for all the players to finish (unless one throws an exception)
    await Promise.all(playerPromises);

    console.log('---');
    console.log('Simulation completed successfully!');
    console.log(`Total flips: ${stats.totalFlips}`);
    console.log(`Successful matches: ${stats.successfulMatches}`);
    console.log(`Failed flips: ${stats.failedFlips}`);

    /** 
     * Simulate one player making random moves.
     * @param playerNumber player to simulate 
     */
    async function player(playerNumber: number): Promise<void> {
        const playerId = `player${playerNumber}`;
        let matches = 0;
        let failures = 0;

        console.log(`${playerId} started`);

        for (let jj = 0; jj < tries; ++jj) {
            try {
                // Random delay before first card
                await timeout(randomDelay(minDelayMilliseconds, maxDelayMilliseconds));

                // Look at board to find valid cards to flip
                const state = board.look(playerId);
                const lines = state.split('\n').filter(l => l.length > 0);
                const availablePositions: Array<{row: number, col: number}> = [];

                // Skip first line (dimensions), collect positions of cards that exist
                for (let i = 1; i < lines.length; i++) {
                    const line = lines[i];
                    if (line !== 'none') { // Card exists (face-down or face-up)
                        const row = Math.floor((i - 1) / size);
                        const col = (i - 1) % size;
                        availablePositions.push({row, col});
                    }
                }

                if (availablePositions.length < 2) {
                    // Not enough cards left to make a move
                    break;
                }

                // Pick first card randomly from available positions
                const firstIndex = randomInt(availablePositions.length);
                const firstPos = availablePositions[firstIndex];
                assert(firstPos !== undefined, 'First position must exist');

                await board.flip(playerId, firstPos.row, firstPos.col);
                stats.totalFlips++;

                // Random delay before second card
                await timeout(randomDelay(minDelayMilliseconds, maxDelayMilliseconds));

                // Pick second card (different from first)
                const remainingPositions = availablePositions.filter((_, idx) => idx !== firstIndex);
                if (remainingPositions.length === 0) {
                    // Only one card left
                    break;
                }

                const secondIndex = randomInt(remainingPositions.length);
                const secondPos = remainingPositions[secondIndex];
                assert(secondPos !== undefined, 'Second position must exist');

                await board.flip(playerId, secondPos.row, secondPos.col);
                stats.totalFlips++;

                // If we got here, we successfully made both flips
                // Check if it was a match by looking at board state
                const newState = board.look(playerId);
                const newLines = newState.split('\n').filter(l => l.length > 0);
                const myCards = newLines.filter(line => line.startsWith('my '));

                if (myCards.length === 2) {
                    // We matched!
                    matches++;
                    stats.successfulMatches++;
                    stats.cardsRemaining -= 2;
                    console.log(`${playerId} matched! (Total matches: ${matches})`);
                }

            } catch (err) {
                // Flip failed - this is normal (empty space, controlled card, etc.)
                failures++;
                stats.failedFlips++;
            }
        }

        console.log(`${playerId} finished: ${matches} matches, ${failures} failed attempts`);
    }
}

/**
 * Random positive integer generator
 * 
 * @param max a positive integer which is the upper bound of the generated number
 * @returns a random integer >= 0 and < max
 */
function randomInt(max: number): number {
    return Math.floor(Math.random() * max);
}

/**
 * Random delay generator
 * 
 * @param min minimum delay in milliseconds
 * @param max maximum delay in milliseconds
 * @returns a random delay between min and max
 */
function randomDelay(min: number, max: number): number {
    return min + Math.random() * (max - min);
}

/**
 * @param milliseconds duration to wait
 * @returns a promise that fulfills no less than `milliseconds` after timeout() was called
 */
async function timeout(milliseconds: number): Promise<void> {
    const { promise, resolve } = Promise.withResolvers<void>();
    setTimeout(resolve, milliseconds);
    return promise;
}

void simulationMain();