/* Copyright (c) 2021-25 MIT 6.102/6.031 course staff, all rights reserved.
 * Redistribution of original or derived work requires permission of course staff.
 */

import assert from 'node:assert';
import fs from 'node:fs';

/**
 * Represents a card on the Memory Scramble board.
 * Immutable.
 */
class Card {
    /**
     * Create a new card.
     * @param text the text/image on the card
     */
    public constructor(
        public readonly text: string
    ) {
        Object.freeze(this);
    }
}

/**
 * Represents a player's state in the game.
 * Mutable.
 */
class PlayerState {
    private readonly controlledCards: Array<{row: number, col: number}> = [];
    private readonly previousNonMatchCards: Array<{row: number, col: number}> = [];
    private previousMatch = false;

    public addCard(row: number, col: number): void {
        this.controlledCards.push({row, col});
    }

    public getControlledCards(): ReadonlyArray<{row: number, col: number}> {
        return [...this.controlledCards];
    }

    public clearCards(): void {
        this.controlledCards.length = 0;
    }

    public hasOneCard(): boolean {
        return this.controlledCards.length === 1;
    }

    public hasTwoCards(): boolean {
        return this.controlledCards.length === 2;
    }

    public setPreviousMatch(match: boolean): void {
        this.previousMatch = match;
    }

    public getPreviousMatch(): boolean {
        return this.previousMatch;
    }

    public setPreviousNonMatchCards(cards: ReadonlyArray<{row: number, col: number}>): void {
        this.previousNonMatchCards.length = 0;
        this.previousNonMatchCards.push(...cards);
    }

    public getPreviousNonMatchCards(): ReadonlyArray<{row: number, col: number}> {
        return [...this.previousNonMatchCards];
    }

    public clearPreviousNonMatchCards(): void {
        this.previousNonMatchCards.length = 0;
    }
}

type Spot = {
    card: Card | null;
    faceUp: boolean;
    controller: string | null;
};

/**
 * Mutable Memory Scramble game board.
 * Supports concurrent play by multiple players.
 */
export class Board {
    private readonly grid: Array<Array<Spot>>;
    private readonly players: Map<string, PlayerState> = new Map();
    private readonly waiters: Map<string, Array<{resolve: () => void, playerId: string}>> = new Map();
    private readonly changeListeners: Array<() => void> = [];

    // Abstraction function:
    //   Represents a Memory Scramble game board with rows x cols grid of cards.
    //   Each spot can have a card (face up/down) or be empty.
    //   Players can control 0, 1, or 2 cards at a time.
    
    // Representation invariant:
    //   - rows > 0 && cols > 0
    //   - grid.length == rows, grid[i].length == cols for all i
    //   - No card is controlled by more than one player
    //   - Face-down cards are not controlled by anyone
    //   - Each player controls 0, 1, or 2 cards
    
    // Safety from rep exposure:
    //   - All fields are private and readonly where possible
    //   - grid is mutable but never returned
    //   - Spot objects are internal and never exposed
    //   - Card objects are immutable

    private constructor(
        private readonly rows: number,
        private readonly cols: number,
        cards: string[]
    ) {
        assert(rows > 0 && cols > 0, 'Board must have positive dimensions');
        assert(cards.length === rows * cols, 'Must have exactly rows*cols cards');
        
        this.grid = [];
        let cardIndex = 0;
        for (let row = 0; row < rows; row++) {
            const gridRow: Array<Spot> = [];
            for (let col = 0; col < cols; col++) {
                const cardText = cards[cardIndex++];
                assert(cardText !== undefined, 'Card must be defined');
                gridRow.push({
                    card: new Card(cardText),
                    faceUp: false,
                    controller: null
                });
            }
            this.grid.push(gridRow);
        }
        
        this.checkRep();
    }

    private checkRep(): void {
        assert(this.rows > 0 && this.cols > 0, 'Dimensions must be positive');
        assert(this.grid.length === this.rows, 'Grid must have correct rows');
        
        const controlledPositions = new Map<string, string>();
        
        for (let row = 0; row < this.rows; row++) {
            const gridRow = this.grid[row];
            assert(gridRow !== undefined, 'Grid row must exist');
            assert(gridRow.length === this.cols, 'Grid row must have correct cols');
            
            for (let col = 0; col < this.cols; col++) {
                const spot = gridRow[col];
                assert(spot !== undefined, 'Spot must exist');
                
                if (!spot.faceUp) {
                    assert(spot.controller === null, 'Face-down card cannot be controlled');
                }
                
                if (spot.controller !== null) {
                    const key = `${row},${col}`;
                    assert(!controlledPositions.has(key), 'Card controlled by multiple players');
                    controlledPositions.set(key, spot.controller);
                    assert(spot.faceUp, 'Controlled card must be face up');
                }
            }
        }
        
        for (const [playerId, state] of this.players) {
            const controlled = state.getControlledCards();
            assert(controlled.length <= 2, 'Player controls at most 2 cards');
            
            for (const {row, col} of controlled) {
                const gridRow = this.grid[row];
                assert(gridRow !== undefined, 'Grid row must exist');
                const spot = gridRow[col];
                assert(spot !== undefined, 'Spot must exist');
                assert(spot.controller === playerId, 'Player state inconsistent with board');
                assert(spot.faceUp, 'Controlled card must be face up');
            }
        }
    }

    /**
     * Create a new Board by parsing a board file.
     *
     * The file format is:
     *   ROWSxCOLS
     *   CARD1
     *   CARD2
     *   ...
     *
     * where ROWS and COLS are positive integers, and each CARD is a non-empty
     * string of non-whitespace characters. The file must contain exactly
     * ROWS * COLS card lines.
     *
     * @param filename - path to the board file to parse
     * @returns Promise that resolves to a new Board with all cards face-down
     * @throws Error if file cannot be read, format is invalid, dimensions are
     *         non-positive, cards contain whitespace, or card count doesn't
     *         match dimensions
     */
    public static async parseFromFile(filename: string): Promise<Board> {
        const content = await fs.promises.readFile(filename, 'utf-8');
        const lines = content.split(/\r?\n/).filter(line => line.length > 0);
        
        if (lines.length < 1) {
            throw new Error('File must have at least dimensions line');
        }
        
        const firstLine = lines[0];
        assert(firstLine !== undefined, 'First line must exist');
        const dimensionsMatch = firstLine.match(/^(\d+)x(\d+)$/);
        if (!dimensionsMatch) {
            throw new Error('First line must be ROWSxCOLS');
        }
        
        const rowsStr = dimensionsMatch[1];
        const colsStr = dimensionsMatch[2];
        assert(rowsStr !== undefined && colsStr !== undefined, 'Dimensions must be captured');
        const rows = parseInt(rowsStr);
        const cols = parseInt(colsStr);
        
        if (rows <= 0 || cols <= 0) {
            throw new Error('Dimensions must be positive');
        }
        
        const cards: string[] = [];
        for (let i = 1; i < lines.length; i++) {
            const line = lines[i];
            assert(line !== undefined, 'Line must exist');
            const card = line.trim();
            if (card.length === 0) {
                throw new Error('Card text cannot be empty');
            }
            if (/[\s\n\r]/.test(card)) {
                throw new Error('Card text cannot contain whitespace or newlines');
            }
            cards.push(card);
        }
        
        if (cards.length !== rows * cols) {
            throw new Error(`Expected ${rows * cols} cards but found ${cards.length}`);
        }
        
        return new Board(rows, cols, cards);
    }

    /**
     * Get the current state of the board from a player's perspective.
     *
     * Returns a string representation of the board showing:
     * - "none" for empty spaces (removed cards)
     * - "down" for face-down cards
     * - "my CARD" for cards controlled by this player
     * - "up CARD" for face-up cards controlled by others or no one
     *
     * Format: ROWSxCOLS\n(SPOT\n)+
     * where SPOT is one of: none, down, "my CARD", "up CARD"
     *
     * @param playerId - the ID of the player requesting the board state
     * @returns string representation of the board from player's perspective
     *
     * Preconditions:
     * - playerId is a non-empty string
     *
     * Postconditions:
     * - Board state is unchanged
     * - Returns valid board state string
     */
    public look(playerId: string): string {
        this.checkRep();
        
        let result = `${this.rows}x${this.cols}\n`;
        
        for (let row = 0; row < this.rows; row++) {
            for (let col = 0; col < this.cols; col++) {
                const gridRow = this.grid[row];
                assert(gridRow !== undefined, 'Grid row must exist');
                const spot = gridRow[col];
                assert(spot !== undefined, 'Spot must exist');
                
                if (spot.card === null) {
                    result += 'none\n';
                } else if (!spot.faceUp) {
                    result += 'down\n';
                } else if (spot.controller === playerId) {
                    result += `my ${spot.card.text}\n`;
                } else {
                    result += `up ${spot.card.text}\n`;
                }
            }
        }
        
        this.checkRep();
        return result;
    }

    /**
     * Flip a card at the specified position for the given player.
     *
     * Implements the Memory Scramble game rules:
     * - First card: Player tries to take control of the card
     *   - If face-down, flips it face-up and controls it
     *   - If face-up and uncontrolled, takes control
     *   - If controlled by another, waits until available
     * - Second card: Player tries to match with their first card
     *   - If match, keeps control of both cards
     *   - If no match, relinquishes control (cards stay face-up)
     *
     * Before flipping a first card, finishes previous move:
     * - If previous cards matched, removes them from board
     * - If previous cards didn't match, turns them face-down
     *
     * @param playerId - the ID of the player making the move
     * @param row - row coordinate (0-indexed from top)
     * @param col - column coordinate (0-indexed from left)
     * @returns Promise<string> resolving to board state from player's perspective
     * @throws Error if position is out of bounds or card is empty (Rule 1-A, 2-A)
     * @throws Error if second card is controlled by another player (Rule 2-B)
     *
     * Preconditions:
     * - playerId is a non-empty string
     * - 0 <= row < rows
     * - 0 <= col < cols
     *
     * Postconditions:
     * - Player controls 0, 1, or 2 cards
     * - Board state updated according to game rules
     * - Previous move completed if this is a first card flip
     */
    public async flip(playerId: string, row: number, col: number): Promise<string> {
        this.checkRep();
        
        if (row < 0 || row >= this.rows || col < 0 || col >= this.cols) {
            throw new Error(`Invalid position: ${row},${col}`);
        }
        
        if (!this.players.has(playerId)) {
            this.players.set(playerId, new PlayerState());
        }
        const player = this.players.get(playerId);
        assert(player !== undefined, 'Player must exist after setting');
        
        if (!player.hasOneCard()) {
            await this.finishPreviousMove(playerId);
        }
        
        if (!player.hasOneCard()) {
            return await this.flipFirstCard(playerId, row, col);
        } else {
            return await this.flipSecondCard(playerId, row, col);
        }
    }

    private async flipFirstCard(playerId: string, row: number, col: number): Promise<string> {
        const gridRow = this.grid[row];
        assert(gridRow !== undefined, 'Grid row must exist');
        const spot = gridRow[col];
        assert(spot !== undefined, 'Spot must exist');
        const player = this.players.get(playerId);
        assert(player !== undefined, 'Player must exist');
        
        if (spot.card === null) {
            throw new Error('Cannot flip: no card at this position');
        }
        
        if (!spot.faceUp) {
            spot.faceUp = true;
            spot.controller = playerId;
            player.addCard(row, col);
            this.notifyChange();
            this.checkRep();
            return this.look(playerId);
        }
        
        if (spot.controller === null) {
            spot.controller = playerId;
            player.addCard(row, col);
            this.checkRep();
            return this.look(playerId);
        }
        
        if (spot.controller !== playerId) {
            await this.waitForCard(row, col, playerId);
            return await this.flipFirstCard(playerId, row, col);
        }
        
        throw new Error('You already control this card');
    }

    private async flipSecondCard(playerId: string, row: number, col: number): Promise<string> {
        const gridRow = this.grid[row];
        assert(gridRow !== undefined, 'Grid row must exist');
        const spot = gridRow[col];
        assert(spot !== undefined, 'Spot must exist');
        const player = this.players.get(playerId);
        assert(player !== undefined, 'Player must exist');
        const firstCards = player.getControlledCards();
        
        if (spot.card === null) {
            // Save first card to turn it down on next move
            player.setPreviousNonMatchCards(firstCards);
            this.relinquishControl(playerId);
            player.clearCards();
            player.setPreviousMatch(false);
            throw new Error('Cannot flip: no card at this position');
        }

        if (spot.controller !== null) {
            // Save first card to turn it down on next move
            player.setPreviousNonMatchCards(firstCards);
            this.relinquishControl(playerId);
            player.clearCards();
            player.setPreviousMatch(false);
            throw new Error('Cannot flip: card is controlled');
        }
        
        if (!spot.faceUp) {
            spot.faceUp = true;
            this.notifyChange();
        }
        
        const firstCardPos = firstCards[0];
        assert(firstCardPos !== undefined, 'First card position must exist');
        const firstGridRow = this.grid[firstCardPos.row];
        assert(firstGridRow !== undefined, 'First card grid row must exist');
        const firstCard = firstGridRow[firstCardPos.col];
        assert(firstCard !== undefined, 'First card must exist');
        const match = firstCard.card?.text === spot.card.text;
        
        if (match) {
            spot.controller = playerId;
            player.addCard(row, col);
            player.setPreviousMatch(true);
        } else {
            // Save positions of BOTH non-matching cards before relinquishing
            const firstCard = player.getControlledCards();
            const bothCards = [...firstCard, {row, col}];
            player.setPreviousNonMatchCards(bothCards);

            this.relinquishControl(playerId);
            player.clearCards();
            player.setPreviousMatch(false);
        }
        
        this.checkRep();
        return this.look(playerId);
    }

    private async finishPreviousMove(playerId: string): Promise<void> {
        const player = this.players.get(playerId);
        if (!player) {
            return;
        }

        const controlled = player.getControlledCards();
        const previousNonMatch = player.getPreviousNonMatchCards();

        if (player.getPreviousMatch() && controlled.length === 2) {
            // Rule 3-A: Remove matched cards (unless there are waiters)
            for (const {row, col} of controlled) {
                const gridRow = this.grid[row];
                assert(gridRow !== undefined, 'Grid row must exist');
                const spot = gridRow[col];
                assert(spot !== undefined, 'Spot must exist');

                // Check if there are waiters for this card
                const key = `${row},${col}`;
                const waitList = this.waiters.get(key);
                const hasWaiters = waitList !== undefined && waitList.length > 0;

                if (hasWaiters) {
                    // Don't remove - relinquish control and notify waiter to take it
                    spot.controller = null;
                    this.notifyWaiters(row, col);
                } else {
                    // No waiters - safe to remove
                    spot.card = null;
                    spot.controller = null;
                }
            }
            player.clearCards();
            player.setPreviousMatch(false);
            this.notifyChange();
        } else if (previousNonMatch.length > 0) {
            // Rule 3-B: Turn face down if not controlled by another player
            const cardsToTurnDown: Array<{row: number, col: number}> = [];

            for (const {row, col} of previousNonMatch) {
                const gridRow = this.grid[row];
                assert(gridRow !== undefined, 'Grid row must exist');
                const spot = gridRow[col];
                assert(spot !== undefined, 'Spot must exist');
                // Only turn down if: card exists, is face up, and NOT controlled by another player
                // "another player" means any player other than this one
                const isControlledByOther = spot.controller !== null && spot.controller !== playerId;
                if (spot.card !== null && spot.faceUp && !isControlledByOther) {
                    cardsToTurnDown.push({row, col});
                }
            }

            // Clear player's previous non-match cards
            player.clearPreviousNonMatchCards();

            // Then turn the cards face down
            for (const {row, col} of cardsToTurnDown) {
                const gridRow = this.grid[row];
                assert(gridRow !== undefined, 'Grid row must exist');
                const spot = gridRow[col];
                assert(spot !== undefined, 'Spot must exist');
                spot.faceUp = false;
                spot.controller = null;
                this.notifyWaiters(row, col);
                this.notifyChange();
            }
        }
    }

    private relinquishControl(playerId: string): void {
        const player = this.players.get(playerId);
        if (!player) {
            return;
        }
        
        for (const {row, col} of player.getControlledCards()) {
            const gridRow = this.grid[row];
            assert(gridRow !== undefined, 'Grid row must exist');
            const spot = gridRow[col];
            assert(spot !== undefined, 'Spot must exist');
            if (spot.controller === playerId) {
                spot.controller = null;
                this.notifyWaiters(row, col);
            }
        }
    }

    private async waitForCard(row: number, col: number, playerId: string): Promise<void> {
        const key = `${row},${col}`;
        
        const {promise, resolve} = Promise.withResolvers<void>();
        
        if (!this.waiters.has(key)) {
            this.waiters.set(key, []);
        }
        const waiterList = this.waiters.get(key);
        assert(waiterList !== undefined, 'Waiter list must exist after setting');
        waiterList.push({resolve, playerId});
        
        await promise;
    }

    private notifyWaiters(row: number, col: number): void {
        const key = `${row},${col}`;
        const waitList = this.waiters.get(key);
        
        if (waitList && waitList.length > 0) {
            const waiter = waitList.shift();
            assert(waiter !== undefined, 'Waiter must exist');
            waiter.resolve();
        }
        
        if (waitList && waitList.length === 0) {
            this.waiters.delete(key);
        }
    }

    /**
     * Apply a transformation function to all cards on the board.
     *
     * Groups cards by their text value and applies the transformation function
     * to each unique card value. All cards with the same text are transformed
     * together atomically to maintain pairwise consistency (if two cards match
     * before the transformation, they will still match after).
     *
     * The transformation does not affect card state (face-up/down, controlled/not).
     * Other operations may interleave with map() while it's running.
     *
     * @param f - async transformation function mapping old card text to new card text.
     *            Must be a mathematical function (same input always produces same output).
     *            Called once per unique card value currently on the board.
     * @returns Promise<void> that resolves when all cards are transformed
     *
     * Preconditions:
     * - f is a pure function (deterministic, no side effects)
     *
     * Postconditions:
     * - All cards transformed according to f
     * - Matching pairs still match after transformation
     * - Card states (face-up/down, control) unchanged
     * - Change listeners notified after each unique value is transformed
     */
    public async map(f: (card: string) => Promise<string>): Promise<void> {
        this.checkRep();
        
        const cardGroups = new Map<string, Array<{row: number, col: number}>>();
        
        for (let row = 0; row < this.rows; row++) {
            for (let col = 0; col < this.cols; col++) {
                const gridRow = this.grid[row];
                assert(gridRow !== undefined, 'Grid row must exist');
                const spot = gridRow[col];
                assert(spot !== undefined, 'Spot must exist');
                if (spot.card !== null) {
                    const text = spot.card.text;
                    if (!cardGroups.has(text)) {
                        cardGroups.set(text, []);
                    }
                    const group = cardGroups.get(text);
                    assert(group !== undefined, 'Group must exist after setting');
                    group.push({row, col});
                }
            }
        }
        
        for (const [oldText, positions] of cardGroups) {
            const newText = await f(oldText);
            
            for (const {row, col} of positions) {
                const gridRow = this.grid[row];
                assert(gridRow !== undefined, 'Grid row must exist');
                const spot = gridRow[col];
                assert(spot !== undefined, 'Spot must exist');
                if (spot.card !== null && spot.card.text === oldText) {
                    spot.card = new Card(newText);
                }
            }
            
            this.notifyChange();
        }
        
        this.checkRep();
    }

    /**
     * Wait for the next change to the board.
     *
     * A change is defined as:
     * - A card flipping face-up or face-down
     * - A card being removed from the board
     * - A card's text changing (via map())
     *
     * Changes in control (player taking/relinquishing control without
     * flipping cards) do NOT trigger this notification.
     *
     * Multiple watchers can wait concurrently. All are notified when
     * a change occurs.
     *
     * @returns Promise<void> that resolves when the board changes
     *
     * Preconditions:
     * - None
     *
     * Postconditions:
     * - Promise resolves exactly once when board changes
     * - If board changes multiple times, only the next change triggers this watcher
     */
    public async waitForChange(): Promise<void> {
        const {promise, resolve} = Promise.withResolvers<void>();
        this.changeListeners.push(resolve);
        await promise;
    }

    private notifyChange(): void {
        const listeners = [...this.changeListeners];
        this.changeListeners.length = 0;
        
        for (const resolve of listeners) {
            resolve();
        }
    }

    /**
     * Get a string representation of the board for debugging.
     *
     * Shows the board as a grid with:
     * - [   ] for empty spaces (removed cards)
     * - [???] for face-down cards
     * - [CARD] for face-up cards, showing their text
     *
     * @returns string representation of the board
     */
    public toString(): string {
        let result = `Board ${this.rows}x${this.cols}\n`;
        for (let row = 0; row < this.rows; row++) {
            for (let col = 0; col < this.cols; col++) {
                const gridRow = this.grid[row];
                assert(gridRow !== undefined, 'Grid row must exist');
                const spot = gridRow[col];
                assert(spot !== undefined, 'Spot must exist');
                if (spot.card === null) {
                    result += '[   ] ';
                } else if (!spot.faceUp) {
                    result += '[???] ';
                } else {
                    result += `[${spot.card.text}] `;
                }
            }
            result += '\n';
        }
        return result;
    }
}