"use strict"

const axios = require("axios");
const neo4j = require('neo4j-driver')

const user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36'

const driver = neo4j.driver("bolt://localhost:7687", neo4j.auth.basic("neo4j", "test"))
// const session = driver.session()

async function getScheduleRange(startDate, endDate) {
    let url = `http://localhost/api/v1/schedule?startDate=${startDate}&endDate=${endDate}&gameType=R`;
    console.log(url)

    let response = await axios({
        url: url,
        headers: { 'User-Agent': user_agent }
    });

    return {
        dates: response.data.dates,
        wait: response.data.wait
    };
}

async function getSchedule(gameDate) {

    console.log(`Processing Single Date: ${gameDate}`);
    let url = `http://localhost/api/v1/schedule?date=${gameDate}&gameType=R`;
    // let url = `http://localhost/api/v1/schedule?date=${gameDate}&teamId=52&gameType=R`;
    // let url = `http://localhost/api/v1/schedule?startDate=2022-10-07&endDate=${gameDate}&gameType=R`;

    let response = await axios({
        url: url,
        headers: { 'User-Agent': user_agent }
    });

    return response.data.dates;
}

async function addGame(gameData) {

    console.log(`Processing Game: ${gameData.gameData.datetime.dateTime} ${gameData.gamePk} - ${gameData.gameData.teams.home.name} (${gameData.liveData.boxscore.teams.home.teamStats.teamSkaterStats.goals}) vs ${gameData.gameData.teams.away.name} (${gameData.liveData.boxscore.teams.away.teamStats.teamSkaterStats.goals})`);
    // console.log(gameData.liveData.linescore.hasShootout)
    let overtime = false;
    // process.exit()
    if ( gameData.liveData.linescore.currentPeriod == 4 ) { 
        overtime = true;
        // console.log(gameData.liveData.linescore.hasShootout);
    }
    

    let session = driver.session()
    try {
        const result = await session.run(
            `MERGE (g:Game {pk: $pk, 
                    season: $season, 
                    gameDate: $gameDate, 
                    overtime: $overtime,
                    shootout: $shootout,
                    away_team: $away.name, 
                    home_team: $home.name, 
                    away_score: $away.stats.goals, 
                    home_score: $home.stats.goals,
                    away_pim: $away.stats.pim, 
                    home_pim: $home.stats.pim,
                    away_shots: $away.stats.shots, 
                    home_shots: $home.stats.shots
                }) 
          ON CREATE 
            SET g.created = timestamp() 
          ON MATCH 
            SET g.lastSeen = timestamp() 
            MERGE (v:Venue {name: $venue})
            MERGE (ht:Team {name: $home.name})
            MERGE (vt:Team {name: $away.name})
            MERGE (g)-[r:PLAYED_AT]->(v)
            MERGE (g)-[rat:AWAY_TEAM]->(vt)
            MERGE (g)-[rht:HOME_TEAM]->(ht)
          RETURN g`,
            {
                pk: gameData.gamePk, 
                season: gameData.gameData.game.season, 
                gameDate: gameData.gameData.datetime.dateTime, 
                overtime: overtime,
                shootout: gameData.liveData.linescore.hasShootout,
                away: {
                    name: gameData.gameData.teams.away.name,
                    stats: gameData.liveData.boxscore.teams.away.teamStats.teamSkaterStats
                },
                home: {
                    name: gameData.gameData.teams.home.name,
                    stats: gameData.liveData.boxscore.teams.home.teamStats.teamSkaterStats
                },
                venue: gameData.gameData.venue.name
            }
        )

        const singleRecord = result.records[0]
        const node = singleRecord.get(0)

    }
    catch (e) {
        console.error(e)
    }
    finally {
        await session.close()
    }
}

async function addOfficials(gameId, officials) {
    let session = driver.session()
    for (const personId of Object.keys(officials)) {
        // console.log(officials[personId])
        const result = await session.run(
            `MATCH
                (game:Game)
            WHERE game.pk = $pk
            MERGE (official:Person {name: $official.fullName, officialType: $officialType})
            MERGE (official)-[r:OFFICIATED]->(game)
            RETURN type(r)`,
            {
                official: officials[personId].official,
                officialType: officials[personId].officialType,
                pk: gameId
            }
        )

    }
    await session.close()
}

async function addSingleGoal(eventData, gameData) {
    let session = driver.session()
    // console.log(gameData.gamePk, eventData.about.eventId)
    await session.run(
        `MATCH
            (game:Game)
        WHERE game.pk = $pk
        MERGE (event:Event {id: $about.eventId, game: $pk})
        RETURN event`,
        {
            result: eventData.result,
            about: eventData.about,
            pk: gameData.gamePk
        }
    )

    await session.close()
    process.exit()
}

async function addSingleEvent(eventData, gameData) {
    let session = driver.session()

    // console.log(eventData.players[0].player.fullName)

    // console.log(gameData.gamePk, eventData.about.eventId)
    // console.log(eventData.result.eventTypeId)
    // process.exit()
    await session.run(
        `MATCH
            (game:Game)
        WHERE game.pk = $pk
        MERGE (event:${eventData.result.eventTypeId} {id: $about.eventId, name: $result.event, game: $pk})
        MERGE (team:Team {name: $team.name})
        MERGE (player:Player {fullName: $players[0].player.fullName})
        MERGE (event)-[r:HAPPENED_IN]->(game)
        MERGE (event)-[rt:BY_TEAM]->(team)
        MERGE (event)-[rp:BY_PLAYER]->(player)
        RETURN event`,
        {
            result: eventData.result,
            about: eventData.about,
            team: eventData.team,
            players: eventData.players,
            pk: gameData.gamePk
        }
    )

    await session.close()
    // process.exit()
}

async function addEvents(gameData) {
    // console.log(gameData.gamePk)
    for (const playEvent of gameData.liveData.plays.allPlays) {
        // console.log(playEvent)
        switch (playEvent.result.event) {
            case "Goal":
                // console.log("Goal")
                // await addSingleGoal(playEvent,gameData)
                break;
            case "Shot":
                // console.log("Shot")
                await addSingleEvent(playEvent, gameData);
                break;
            case "Missed Shot":
                // console.log("Missed Shot")
                await addSingleEvent(playEvent, gameData);
                break;
            case "Blocked Shot":
                // console.log("Missed Shot")
                // console.log(playEvent.players)
                // process.exit()
                await addSingleEvent(playEvent, gameData);
                break;
            case "Giveaway":
                // console.log("Missed Shot")
                await addSingleEvent(playEvent, gameData);
                break;
            case "Penalty":
                // console.log("Hit")
                await addSingleEvent(playEvent, gameData);
                break;
            case "Hit":
                // console.log("Hit")
                await addSingleEvent(playEvent, gameData);
                break;
            case "Faceoff":
                // console.log("Faceoff")
                await addSingleEvent(playEvent, gameData);
                break;
            case "Takeaway":
                // console.log("Faceoff")
                await addSingleEvent(playEvent, gameData);
                break;
            default:
                // console.log(playEvent.result.event)
                // await addSingleEvent(playEvent,gameData);
                break;
        }

        // process.exit()
    }
}

async function addScoringEvents(gameData) {
    // console.log(gameData.gamePk)
    // console.log(gameData.liveData)
    let session = driver.session()




    for (const scoringId of gameData.liveData.plays.scoringPlays) {
        // console.log(gameData.liveData.plays.allPlays[scoringId].team)
        // process.exit()
        // console.log(gameData.liveData.plays.allPlays[scoringId].about.eventId, gameData.liveData.plays.allPlays[scoringId].players[0].player.fullName);
        // process.exit()
        gameData.liveData.plays.allPlays[scoringId].result.emptyNet = gameData.liveData.plays.allPlays[scoringId].result.emptyNet ? gameData.liveData.plays.allPlays[scoringId].result.emptyNet : false
        const result = await session.run(
            `MATCH
                (game:Game)
            WHERE game.pk = $pk
            MERGE (event:Event {id: $about.eventId, game: $pk, name: $result.event, gameWinningGoal: $result.gameWinningGoal, emptyNet: $result.emptyNet, time: $about.dateTime, period: $about.period})
            MERGE (team:Team {name: $team.name})
            MERGE (event)-[r:HAPPENED_IN]->(game)
            MERGE (event)-[rt:SCORED_BY_TEAM]->(team)
            RETURN type(r)`,
            {
                result: gameData.liveData.plays.allPlays[scoringId].result,
                team: gameData.liveData.plays.allPlays[scoringId].team,
                about: gameData.liveData.plays.allPlays[scoringId].about,
                pk: gameData.gamePk
            }
        )
        for (const player of gameData.liveData.plays.allPlays[scoringId].players) {
            // console.log(player)
            switch (player.playerType) {
                case "Scorer":
                    var scoredBy = player.player.fullName
                    await session.run(
                        `MATCH
                            (game:Game)
                        WHERE game.pk = $pk
                        MERGE (event:Event {id: $about.eventId, game: $pk})
                        MERGE (player:Player {fullName: $player.player.fullName})
                        MERGE (event)-[rt:SCORED_BY {Game: $pk}]->(player)
                        RETURN player`,
                        {
                            result: gameData.liveData.plays.allPlays[scoringId].result,
                            about: gameData.liveData.plays.allPlays[scoringId].about,
                            player: player,
                            pk: gameData.gamePk
                        }
                    )
                    break;
                case "Goalie":
                    var scoredAgainst = player.player.fullName
                    await session.run(
                        `MATCH
                            (game:Game)
                        WHERE game.pk = $pk
                        MERGE (event:Event {id: $about.eventId, game: $pk})
                        MERGE (player:Player {fullName: $player.player.fullName})
                        MERGE (event)-[rt:SCORED_ON]->(player)
                        RETURN player`,
                        {
                            result: gameData.liveData.plays.allPlays[scoringId].result,
                            about: gameData.liveData.plays.allPlays[scoringId].about,
                            player: player,
                            pk: gameData.gamePk
                        }
                    )
                    break;
                case "Assist":
                    await session.run(
                        `MATCH
                            (game:Game)
                        WHERE game.pk = $pk
                        MERGE (event:Event {id: $about.eventId, game: $pk})
                        MERGE (player:Player {fullName: $player.player.fullName})
                        MERGE (event)-[rt:ASSISTED_BY {Game: $pk}]->(player)
                        RETURN player`,
                        {
                            result: gameData.liveData.plays.allPlays[scoringId].result,
                            about: gameData.liveData.plays.allPlays[scoringId].about,
                            player: player,
                            pk: gameData.gamePk
                        }
                    )
                    break;
                default:
                    break;
            }
        }
    }
    // process.exit()
    await session.close()
}

async function addPlayers(gameData) {
    let session = driver.session()
    var homeTeam = gameData.gameData.teams.home;

    for (const playerId of Object.keys(gameData.gameData.players)) {
        if (!gameData.gameData.players[playerId].nationality) {
            gameData.gameData.players[playerId].nationality = gameData.gameData.players[playerId].birthCountry
        }
        if (!gameData.gameData.players[playerId].weight) {
            gameData.gameData.players[playerId].weight = 0
        }

        if (!gameData.gameData.players[playerId].height) {
            gameData.gameData.players[playerId].height = 0
        }

        let playerStats;
        let currentTeam;
        if ( gameData.liveData.boxscore.teams.home.players[playerId] ) { 
            playerStats = gameData.liveData.boxscore.teams.home.players[playerId].stats;
            currentTeam = gameData.gameData.teams.home
        } else { 
            playerStats = gameData.liveData.boxscore.teams.away.players[playerId].stats;
            currentTeam = gameData.gameData.teams.away
        }

        if (!gameData.gameData.players[playerId].currentTeam ) { 
            gameData.gameData.players[playerId].currentTeam = currentTeam;
        } 

        if (gameData.gameData.players[playerId].currentTeam) {
            try {
                const result = await session.run(
                    `MERGE (p:Player {id: $player.id, fullName: $player.fullName, birthDate: $player.birthDate, height: $player.height, weight: $player.weight, position: $player.primaryPosition.code}) 
                  ON CREATE 
                    SET p.created = timestamp() 
                  ON MATCH 
                    SET p.lastSeen = timestamp() 
                    MERGE (t:Team {name: $player.currentTeam.name})
                    MERGE (g:Game {pk: $gameId})
                    // MATCH (g:Game)
                    MERGE (c:Country {name: $player.nationality})
                    MERGE (p)-[rn:NATIONALITY]->(c)
                    MERGE (p)-[rt:PLAYS_FOR]->(t)
                    MERGE (p)-[pg:PLAYS_IN]->(g)
                  RETURN p`,
                    {
                        player: gameData.gameData.players[playerId],
                        gameId: gameData.gamePk
                    }
                )
            } catch (e) {
                console.error(e)
                console.log(gameData.gameData.players[playerId])
                process.exit()
            }

        } else {
            console.log(`Unable to process the following player: ${gameData.gameData.players[playerId].fullName} ${gameData.gameData.players[playerId].link}`)
        }

    }
    await session.close()
}

async function getGameEvents(gameLink) {
    let response = await axios({
        url: `http://localhost${gameLink}`,
        headers: { 'User-Agent': user_agent }
    });

    return response.data;

}


async function addVenue(venueData, gamePk) {
    // console.log(venueData,gamePk)
    // process.exit()
    let session = driver.session()
    try {
        const result = await session.run(
            `MATCH (game:Game)
            WHERE game.pk = $gamePk
            MERGE (v:Venue {name: $name}) 
            MERGE (game)-[rn:PLAYED_AT]->(v)
            ON CREATE 
                SET v.created = timestamp() 
            ON MATCH 
                SET v.lastSeen = timestamp() 
            RETURN v`,
            { name: venueData.name, gamePk: gamePk }
        )
    } finally {
        await session.close()
    }
}

function join(t, a, s) {
    function format(m) {
        let f = new Intl.DateTimeFormat('en', m);
        return f.format(t);
    }
    return a.map(format).join(s);
}

var delay = (time) => {
    // console.log(time)
    return new Promise(res => {
        setTimeout(res, time)
    })
}

(async () => {
    let a = [{ year: 'numeric' }, { month: 'numeric' }, { day: 'numeric' }];
    // var startDate = new Date(2023, 0, 12) // start of nhl season
    var startDate = new Date(Date.parse("Mar 08, 2023"));
    // process.exit()
    // var startDate = new Date(2023, 0, 7) // start of nhl season
    var endDate = new Date(); // Now
    endDate.setDate(endDate.getDate())
    startDate = join(startDate, a, '-');
    endDate = join(endDate, a, '-');
    var gamesSchedule = await getScheduleRange(startDate, endDate);
    for (const gameDay of gamesSchedule.dates) {
        if (gameDay.games) {
            for (const game of gameDay.games) {
                if (game !== undefined && game.status.abstractGameState == 'Final') {
                    let gameFeed = await getGameEvents(game.link)
                    await addGame(gameFeed)
                    await addVenue(gameFeed.gameData.venue, game.gamePk)
                    await addOfficials(game.gamePk, gameFeed.liveData.boxscore.officials);
                    await addPlayers(gameFeed)
                    await addEvents(gameFeed)
                    await addScoringEvents(gameFeed)
                    // await delay(gamesSchedule.wait * 1)
                }
            }
        }

    }

    // on application exit:
    await driver.close()
    // console.log("foo")
})();
