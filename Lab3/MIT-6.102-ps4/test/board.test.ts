/* Copyright (c) 2021-25 MIT 6.102/6.031 course staff, all rights reserved.
 * Redistribution of original or derived work requires permission of course staff.
 */

import assert from 'node:assert';
import fs from 'node:fs';
import { Board } from '../src/board.js';

/**
 * Tests for the Board abstract data type.
 */
describe('Board', function() {
    
    // Testing strategy omitted for brevity - see original
    
    describe('parseFromFile', function() {
        
        it('parses valid 3x3 board', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            const state = board.look('test');
            assert(state.startsWith('3x3\n'));
            const lines = state.split('\n');
            assert.strictEqual(lines.length, 11);
            assert(lines.slice(1, 10).every(line => line === 'down'));
        });
        
        it('parses valid 5x5 board', async function() {
            const board = await Board.parseFromFile('boards/ab.txt');
            const state = board.look('test');
            assert(state.startsWith('5x5\n'));
            const lines = state.split('\n');
            assert.strictEqual(lines.length, 27);
        });
        
        it('throws on invalid file', async function() {
            await assert.rejects(
                async () => await Board.parseFromFile('nonexistent.txt')
            );
        });
    });
    
    describe('Rule 1-A: flip empty space fails', function() {
        it('fails when flipping removed card', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 1);
            await board.flip('alice', 1, 0);
            
            await assert.rejects(
                async () => await board.flip('bob', 0, 0),
                /no card/i
            );
        });
    });
    
    describe('Rule 1-B: flip face-down card succeeds', function() {
        it('flips face-down card and player controls it', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            const state = await board.flip('alice', 0, 0);
            
            const lines = state.split('\n');
            const line1 = lines[1];
            assert(line1 !== undefined, 'Line 1 must exist');
            assert(line1.startsWith('my '), 'First card should be controlled by alice');
        });
    });
    
    describe('Rule 1-C: flip face-up uncontrolled card', function() {
        it('takes control of face-up uncontrolled card', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 2);
            
            const state = await board.flip('bob', 0, 0);
            const lines = state.split('\n');
            const line1 = lines[1];
            assert(line1 !== undefined, 'Line 1 must exist');
            assert(line1.startsWith('my '), 'Bob should control the card');
        });
    });
    
    describe('Rule 1-D: wait for card controlled by another', function() {
        it('waits when card controlled by another player', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            // Alice flips (0,0) = 🦄 as first card
            await board.flip('alice', 0, 0);
            
            let bobGotCard = false;
            
            // Bob tries to flip same card - should wait
            const bobPromise = board.flip('bob', 0, 0).then(() => {
                bobGotCard = true;
            }).catch(() => {
                // Bob might fail if card is removed
                bobGotCard = false;
            });
            
            // Give Bob a moment (should still be waiting)
            await timeout(10);
            assert.strictEqual(bobGotCard, false, 'Bob should still be waiting');
            
            // Alice flips (0,2) = 🌈 as second (no match)
            await board.flip('alice', 0, 2);
            
            // Now Bob should be able to get the card (Alice relinquished)
            await bobPromise;
            
            // Bob should have gotten the card
            const bobState = board.look('bob');
            assert(bobState.includes('my '), 'Bob should control a card');
        });
    });
    
    describe('Rule 2-A: second flip on empty space fails', function() {
        it('fails and relinquishes first card', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 1);
            await board.flip('alice', 1, 0);
            
            await board.flip('bob', 1, 1);
            
            await assert.rejects(
                async () => await board.flip('bob', 0, 0),
                /no card/i
            );
            
            const state = board.look('bob');
            const lines = state.split('\n');
            const bobCards = lines.filter(line => line.startsWith('my '));
            assert.strictEqual(bobCards.length, 0, 'Bob should control no cards');
        });
    });
    
    describe('Rule 2-B: second flip on controlled card fails', function() {
        it('fails immediately when second card is controlled', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('bob', 1, 0);
            
            await assert.rejects(
                async () => await board.flip('bob', 0, 0),
                /controlled/i
            );
        });
        
        it('fails immediately when flipping own first card as second', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            
            await assert.rejects(
                async () => await board.flip('alice', 0, 0),
                /controlled/i
            );
        });
    });
    
    describe('Rule 2-D: matching second card', function() {
        it('keeps control of matching pair', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 1);
            
            const state = board.look('alice');
            const lines = state.split('\n');
            const aliceCards = lines.filter(line => line.startsWith('my '));
            assert.strictEqual(aliceCards.length, 2, 'Alice should control 2 cards');
        });
    });
    
    describe('Rule 2-E: non-matching second card', function() {
        it('relinquishes control when cards dont match', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 2);
            
            const state = board.look('alice');
            const lines = state.split('\n');
            const aliceCards = lines.filter(line => line.startsWith('my '));
            assert.strictEqual(aliceCards.length, 0, 'Alice should control no cards');
            
            const line1 = lines[1];
            const line3 = lines[3];
            assert(line1 !== undefined && line3 !== undefined, 'Lines must exist');
            assert(line1.startsWith('up '), 'First card should be face up');
            assert(line3.startsWith('up '), 'Second card should be face up');
        });
    });
    
    describe('Rule 3-A: matched pair removed on next move', function() {
        it('removes matched cards when player makes new first flip', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 1);
            await board.flip('alice', 1, 0);
            
            const state = board.look('alice');
            const lines = state.split('\n');
            assert.strictEqual(lines[1], 'none', 'First card should be removed');
            assert.strictEqual(lines[2], 'none', 'Second card should be removed');
        });
    });
    
    describe('Rule 3-B: non-matching cards turned face down', function() {
        it('turns face down if not controlled by another', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 2);
            await board.flip('alice', 1, 0);
            
            const state = board.look('alice');
            const lines = state.split('\n');
            assert.strictEqual(lines[1], 'down', 'First card should be face down');
            assert.strictEqual(lines[3], 'down', 'Second card should be face down');
        });
        
        it('doesnt turn face down if controlled by another', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 2);
            await board.flip('bob', 0, 0);
            await board.flip('alice', 1, 0);
            
            const aliceState = board.look('alice');
            const aliceLines = aliceState.split('\n');
            const line1 = aliceLines[1];
            assert(line1 !== undefined, 'Line must exist');
            assert(line1.startsWith('up '), 'Card controlled by Bob should stay up');
            assert.strictEqual(aliceLines[3], 'down', 'Uncontrolled card should be face down');
        });
    });
    
    describe('Concurrent play', function() {
        it('handles two players making moves sequentially', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 1);
            await board.flip('bob', 0, 2);
            await board.flip('bob', 1, 0);
            
            const aliceState = board.look('alice');
            const bobState = board.look('bob');
            
            assert(aliceState.includes('my '));
            assert(bobState.includes('my '));
        });
        
        it('handles multiple waiters for same card', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            
            let bobDone = false;
            let charlieDone = false;
            
            const bobPromise = board.flip('bob', 0, 0).then(() => { bobDone = true; });
            const charliePromise = board.flip('charlie', 0, 0).then(() => { charlieDone = true; });
            
            await timeout(10);
            assert(!bobDone && !charlieDone, 'Both should be waiting');
            
            await board.flip('alice', 0, 1);
            await board.flip('alice', 1, 0);
            
            await Promise.race([bobPromise, charliePromise]);
            await timeout(10);
            assert(bobDone || charlieDone, 'One player should have gotten card');
        });
    });
    
    describe('map()', function() {
        it('transforms all cards', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.map(async (card) => card + '!');
            
            const state = await board.flip('alice', 0, 0);
            const lines = state.split('\n');
            const line1 = lines[1];
            assert(line1 !== undefined, 'Line must exist');
            assert(line1.includes('!'), 'Card should be transformed');
        });
        
        it('maintains pairwise consistency', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.map(async (card) => {
                if (card === '🦄') {
                    return '🐴';
                }
                return card;
            });
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 1);
            
            const state = board.look('alice');
            const lines = state.split('\n');
            const line1 = lines[1];
            const line2 = lines[2];
            assert(line1 !== undefined && line2 !== undefined, 'Lines must exist');
            assert(line1.startsWith('my '), 'Should still control cards (matched)');
            assert(line2.startsWith('my '), 'Should still control cards (matched)');
        });
    });
    
    describe('watch()', function() {
        it('waits for board change', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            let watchDone = false;
            const watchPromise = board.waitForChange().then(() => { watchDone = true; });
            
            await timeout(10);
            assert(!watchDone, 'Watch should be waiting');
            
            await board.flip('alice', 0, 0);
            
            await watchPromise;
            assert(watchDone, 'Watch should complete after change');
        });
        
        it('detects card removal', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('alice', 0, 1);
            
            const watchPromise = board.waitForChange();
            
            await board.flip('alice', 1, 0);
            
            await watchPromise;
        });
    });
    
    describe('look()', function() {
        it('shows correct perspective', async function() {
            const board = await Board.parseFromFile('boards/perfect.txt');
            
            await board.flip('alice', 0, 0);
            await board.flip('bob', 0, 2);
            
            const aliceView = board.look('alice');
            const bobView = board.look('bob');
            
            const aliceLines = aliceView.split('\n');
            const line1 = aliceLines[1];
            const line3 = aliceLines[3];
            assert(line1 !== undefined && line3 !== undefined, 'Lines must exist');
            assert(line1.startsWith('my '), 'Alice sees her card as my');
            assert(line3.startsWith('up '), 'Alice sees Bob\'s card as up');
            
            const bobLines = bobView.split('\n');
            const bLine1 = bobLines[1];
            const bLine3 = bobLines[3];
            assert(bLine1 !== undefined && bLine3 !== undefined, 'Lines must exist');
            assert(bLine1.startsWith('up '), 'Bob sees Alice\'s card as up');
            assert(bLine3.startsWith('my '), 'Bob sees his card as my');
        });
    });
});

async function timeout(milliseconds: number): Promise<void> {
    const { promise, resolve } = Promise.withResolvers<void>();
    setTimeout(resolve, milliseconds);
    return promise;
}