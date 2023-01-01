"use strict"

const axios = require("axios");
const neo4j = require('neo4j-driver')

const user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36'

const driver = neo4j.driver("bolt://localhost:7687", neo4j.auth.basic("neo4j", "test"))
// const session = driver.session()

async function getSchedule(gameDate) {
    let url = `http://localhost/api/v1/schedule?date=${gameDate}`;
    // let url = `http://localhost/api/v1/schedule?startDate=2022-10-07&endDate=${gameDate}`;

    let response = await axios({
        url: url,
        headers: { 'User-Agent': user_agent }
    });

    // console.log(response.data.dates[0].games[0]);
    // if ( response.data.dates && response.data.dates[0] && response.data.dates[0].games ) { 
    return response.data.dates
    // }
    // else { 
    //     return undefined
    // }

}

async function addGame(gameData) {
    // console.log(gameData)
    // console.log(gameData.venue.id)
    // if ( gameData.venue.id == undefined ) { 
    //     console.log(gameData)
    // }
    // process.exit()
    // return true;
    let session = driver.session()
    try {
        const result = await session.run(
            `MERGE (g:Game {pk: $pk, season: $season, gameDate: $gameDate, away_team: $away_team, home_team: $home_team, away_score: $away_score, home_score: $home_score  }) 
          ON CREATE 
            SET g.created = timestamp() 
          ON MATCH 
            SET g.lastSeen = timestamp() 
            MERGE (v:Venue {name: $venue})
            MERGE (ht:Team {name: $home_team})
            MERGE (vt:Team {name: $away_team})
            MERGE (g)-[r:PLAYED_AT]->(v)
            MERGE (g)-[rat:AWAY_TEAM]->(vt)
            MERGE (g)-[rht:HOME_TEAM]->(ht)
          RETURN g`,
            {
                pk: gameData.gamePk, season: gameData.season, gameDate: gameData.gameDate, away_team: gameData.teams.away.team.name, home_team: gameData.teams.home.team.name, away_score: gameData.teams.away.score, home_score: gameData.teams.home.score,
                venue: gameData.venue.name
            }
        )

        const singleRecord = result.records[0]
        const node = singleRecord.get(0)

        // console.log(node.properties.name)
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

        // const result = await session.run(
        //     `MATCH
        //         (official:Person {name: $official.fullName}),
        //         (game:Game {pk: $pk})
        //     MERGE (official)-[r:OFFICIATED]->(game)
        //     RETURN official.name`,
        //     {
        //         official: gameData.liveData.boxscore.officials[personId].official,
        //         officialType: gameData.liveData.boxscore.officials[personId].officialType,
        //         pk: gameData.gamePk
        //     }
        // )
        // const result = await session.run(
        //     `MERGE (p:Person {id: $official.id, fullName: $official.fullName, officialType: $officialType}) 
        //     ON CREATE 
        //         SET p.created = timestamp() 
        //     ON MATCH 
        //         SET p.lastSeen = timestamp() 
        //         MERGE (g:Game {name: $currentTeam.name})
        //         // MATCH (g:Game)
        //         MERGE (c:Country {name: $nationality})
        //         MERGE (p)-[rn:NATIONALITY]->(c)
        //         MERGE (p)-[rt:PLAYS_FOR]->(t)
        //     RETURN p`,
        //     gameData.liveData.boxscore.officials[personId]
        // )
    }
    await session.close()
}

async function addEvents(gameData) {
    console.log(gameData.gamePk)
    // console.log(gameData.liveData)
    // console.log(gameData.gameData.players)
    // if ( gameData.venue.id == undefined ) { 
    //     console.log(gameData)
    // }
    // process.exit()
    // return true;
    let session = driver.session()


    // If Jets (#52)
    if (gameData.gameData.teams.away.id == 52 || gameData.gameData.teams.home.id == 52) {
        // console.log("Jets")

        // Referee and Linesman
        // await addOfficials(gameData.gamePk, gameData.liveData.boxscore.officials);
    }
    /*
    for (const playerId of Object.keys(gameData.gameData.players)) {
        // console.log(playerId)
        if ( gameData.gameData.players[playerId].currentTeam ) { 
            
            const result = await session.run(
                `MERGE (p:Player {id: $id, fullName: $fullName, primaryNumber: $primaryNumber, birthDate: $birthDate, height: $height, weight: $weight, position: $primaryPosition.code}) 
              ON CREATE 
                SET p.created = timestamp() 
              ON MATCH 
                SET p.lastSeen = timestamp() 
                MERGE (t:Team {name: $currentTeam.name})
                // MATCH (g:Game)
                MERGE (c:Country {name: $nationality})
                MERGE (p)-[rn:NATIONALITY]->(c)
                MERGE (p)-[rt:PLAYS_FOR]->(t)
              RETURN p`,
                gameData.gameData.players[playerId]
            )
        } else { 
            console.log(`Unable to process the following player: ${gameData.gameData.players[playerId].fullName} ${gameData.gameData.players[playerId].link}`)
        }
        
    }
    */
    await session.close()
}

async function addScoringEvents(gameData) {
    console.log(gameData.gamePk)
    // console.log(gameData.liveData)
    let session = driver.session()

    // console.log(gameData.liveData.plays.scoringPlays)
    for (const scoringId of gameData.liveData.plays.scoringPlays) {
        // console.log(gameData.liveData.plays.allPlays[scoringId]);
        gameData.liveData.plays.allPlays[scoringId].result.emptyNet = gameData.liveData.plays.allPlays[scoringId].result.emptyNet ? gameData.liveData.plays.allPlays[scoringId].result.emptyNet : false
        const result = await session.run(
            `MATCH
                (game:Game)
            WHERE game.pk = $pk
            MERGE (event:Event {event: $result.event, gameWinningGoal: $result.gameWinningGoal, emptyNet: $result.emptyNet, time: $about.dateTime, period: $about.period})
            MERGE (team:Team {name: $team.name})
            MERGE (event)-[r:HAPPENED_IN]->(game)
            MERGE (event)-[rt:SCORED_BY]->(team)
            RETURN type(r)`,
            {
                result: gameData.liveData.plays.allPlays[scoringId].result,
                team: gameData.liveData.plays.allPlays[scoringId].team,
                about: gameData.liveData.plays.allPlays[scoringId].about,
                pk: gameData.gamePk
            }
        )
    }
    // process.exit()
    /*
    for (const playerId of Object.keys(gameData.gameData.players)) {
        // console.log(playerId)
        if ( gameData.gameData.players[playerId].currentTeam ) { 
            
            const result = await session.run(
                `MERGE (p:Player {id: $id, fullName: $fullName, primaryNumber: $primaryNumber, birthDate: $birthDate, height: $height, weight: $weight, position: $primaryPosition.code}) 
              ON CREATE 
                SET p.created = timestamp() 
              ON MATCH 
                SET p.lastSeen = timestamp() 
                MERGE (t:Team {name: $currentTeam.name})
                // MATCH (g:Game)
                MERGE (c:Country {name: $nationality})
                MERGE (p)-[rn:NATIONALITY]->(c)
                MERGE (p)-[rt:PLAYS_FOR]->(t)
              RETURN p`,
                gameData.gameData.players[playerId]
            )
        } else { 
            console.log(`Unable to process the following player: ${gameData.gameData.players[playerId].fullName} ${gameData.gameData.players[playerId].link}`)
        }
        
        // process.exit()

        // process.exit()
    }
    */
    await session.close()
}

async function addPlayers(gameData) {
    // console.log(gameData.gamePk)
    // console.log(gameData.liveData)
    let session = driver.session()

    for (const playerId of Object.keys(gameData.gameData.players)) {
        // console.log(gameData.gameData.players[playerId])
        // process.exit()
        if ( !gameData.gameData.players[playerId].nationality ) { 
            gameData.gameData.players[playerId].nationality = gameData.gameData.players[playerId].birthCountry
        }
        if ( !gameData.gameData.players[playerId].weight ) { 
            gameData.gameData.players[playerId].weight = 0
        }

        if ( !gameData.gameData.players[playerId].height ) { 
            gameData.gameData.players[playerId].height = 0
        }
        
        if ( gameData.gameData.players[playerId].currentTeam ) { 
            
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
        
        // process.exit()

        // process.exit()
    }
    await session.close()
}

async function getGameEvents(gameId) {
    let response = await axios({
        url: `http://localhost/api/v1/game/${gameId}/feed/live`,
        headers: { 'User-Agent': user_agent }
    });

    // console.log(response.data.liveData.boxscore.officials);
    // console.log(`http://localhost/api/v1/game/${gameId}/feed/live`);
    // process.exit();
    return response.data;

}


async function addVenue(venueData) {
    console.log(venueData)
    // try {
    //     const result = await session.run(
    //         `MERGE (g:Game {name: $name}) 
    //       ON CREATE 
    //         SET g.created = timestamp() 
    //       ON MATCH 
    //         SET g.lastSeen = timestamp() 
    //       RETURN g`,
    //         { name: personName }
    //     )

    //     const singleRecord = result.records[0]
    //     const node = singleRecord.get(0)

    //     console.log(node.properties.name)
    // } finally {
    //     await session.close()
    // }
}

function join(t, a, s) {
    function format(m) {
        let f = new Intl.DateTimeFormat('en', m);
        return f.format(t);
    }
    return a.map(format).join(s);
}


(async () => {
    let a = [{ year: 'numeric' }, { month: 'numeric' }, { day: 'numeric' }];
    // console.log(new Date(2022, 9, 7))
    for (var d = new Date(2022, 9, 7); d <= Date.now(); d.setDate(d.getDate() + 1)) {
        // console.log(new Date(d))
        // var leDate = new Date()
        var leDate = d;
        // leDate.setDate(leDate.getDate() - 13)
        let s = join(leDate, a, '-');
        // console.log(s);
        // process.exit()

        var gameSchedule = await getSchedule(s);
        // console.log(gameSchedule)
        for (const gameDay of gameSchedule) {
            // console.log(gameDay)
            if (gameDay.games) {
                for (const game of gameDay.games) {
                    // if ( game.status.abstractGameState == 'Final') {
                    //     console.log(game)
                    //     process.exit()
                    // }

                    if (game !== undefined && game.status.abstractGameState == 'Final') {
                        let gameFeed = await getGameEvents(game.gamePk)
                        await addGame(game)
                        await addOfficials(game.gamePk,gameFeed.liveData.boxscore.officials);
                        await addPlayers(gameFeed)
                        // await addEvents(gameFeed)
                        await addScoringEvents(gameFeed)
                    }
                }
            }

        }

    }


    // on application exit:
    await driver.close()
    // console.log("foo")
})();

