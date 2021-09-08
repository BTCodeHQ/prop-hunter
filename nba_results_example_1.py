import requests
import time
import sys
import pandas as pd
from datetime import date, datetime, timedelta
import psycopg2
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert
today = date.today()
NBAdate = (today - timedelta(1)).strftime('%Y%m%d')
SQLdate = today.strftime('%Y-%m-%d')

void_players = []
def get_results():
    #root = 'http://data.nba.net/10s/prod/v1/today.json'
    boxscore_cols = ['Head To Head','Winning Team & Margin','1st Half Winner','1st Quarter Winner','2nd Quarter Winner',
                '3rd Quarter Winner','4th Quarter Winner','Will There Be Overtime?','Both Teams to Score 100+ Points',
                'Both Teams to Score 110+ Points','Both Teams to Score 120+ Points','Team With Highest Scoring Quarter','Top Points Scorer','To Score 10+ Points',
                'To Score 15+ Points','To Score 20+ Points','To Score 25+ Points','To Score 30+ Points','To Score 35+ Points','To Score 40+ Points','To Score 45+ Points',
                'To Score 50+ Points','To Record 4+ Rebounds','To Record 6+ Rebounds','To Record 8+ Rebounds','To Record 10+ Rebounds','To Record 12+ Rebounds',
                'To Record 14+ Rebounds','To Record 16+ Rebounds','To Record 18+ Rebounds','To Record 20+ Rebounds',
                'To Record 4+ Assists','To Record 6+ Assists','To Record 8+ Assists','To Record 10+ Assists','To Record 12+ Assists','To Record 14+ Assists','To Record 1+ Steals',
                'To Record 2+ Steals','To Record 3+ Steals','To Record 4+ Steals','To Record 5+ Steals','To Record 1+ Blocks','To Record 2+ Blocks','To Record 3+ Blocks',
                'To Record 4+ Blocks','To Record 5+ Blocks','To Make 1+ Three Point FG','To Make 2+ Three Point FG','To Make 3+ Three Point FG',
                'To Make 4+ Three Point FG','To Make 5+ Three Point FG','To Record A Double Double','To Record A Triple Double']
    pbp_cols = ['First Basket','First Team To Score','First Team Basket Scorer','Race To 8 Points','Race To 10 Points',
                'Race To 15 Points','Race To 20 Points','Race To 25 Points','Race To 30 Points','Last Team To Score']

    nbaTeams = {'1610612737': 'Atlanta Hawks',
                '1610612738': 'Boston Celtics',
                '1610612751': 'Brooklyn Nets',
                '1610612766': 'Charlotte Hornets',
                '1610612741': 'Chicago Bulls',
                '1610612739': 'Cleveland Cavaliers',
                '1610612742': 'Dallas Mavericks',
                '1610612743': 'Denver Nuggets',
                '1610612765': 'Detroit Pistons',
                '1610612744': 'Golden State Warriors',
                '1610612745': 'Houston Rockets',
                '1610612754': 'Indiana Pacers',
                '1610612746': 'Los Angeles Clippers',
                '1610612747': 'Los Angeles Lakers',
                '1610612763': 'Memphis Grizzlies',
                '1610612748': 'Miami Heat',
                '1610612749': 'Milwaukee Bucks',
                '1610612750': 'Minnesota Timberwolves',
                '1610612740': 'New Orleans Pelicans',
                '1610612752': 'New York Knicks',
                '1610612760': 'Oklahoma City Thunder',
                '1610612753': 'Orlando Magic',
                '1610612755': 'Philadelphia 76ers',
                '1610612756': 'Phoenix Suns',
                '1610612757': 'Portland Trail Blazers',
                '1610612758': 'Sacramento Kings',
                '1610612759': 'San Antonio Spurs',
                '1610612761': 'Toronto Raptors',
                '1610612762': 'Utah Jazz',
                '1610612764': 'Washington Wizards'}
    nbaPlayers = {'1630173': 'Precious Achiuwa',
                '1629121': 'Jaylen Adams',
                '203500': 'Steven Adams',
                '1628389': 'Bam Adebayo',
                '200746': 'LaMarcus Aldridge',
                '1630234': 'Ty-Shon Alexander',
                '1629638': 'Nickeil Alexander-Walker',
                '1628960': 'Grayson Allen',
                '1628386': 'Jarrett Allen',
                '202329': 'Al-Farouq Aminu',
                '203937': 'Kyle Anderson',
                '203507': 'Giannis Antetokounmpo',
                '1628961': 'Kostas Antetokounmpo',
                '203648': 'Thanasis Antetokounmpo',
                '2546': 'Carmelo Anthony',
                '1630175': 'Cole Anthony',
                '1628384': 'OG Anunoby',
                '1627853': 'Ryan Arcidiacono',
                '2772': 'Trevor Ariza',
                '201571': 'DJ Augustin',
                '1630166': 'Deni Avdija',
                '1629028': 'Deandre Ayton',
                '1628962': 'Udoka Azubuike',
                '1628407': 'Dwayne Bacon',
                '1628963': 'Marvin Bagley III',
                '1630163': 'LaMelo Ball',
                '1628366': 'Lonzo Ball',
                '1628964': 'Mo Bamba',
                '1630217': 'Desmond Bane',
                '203084': 'Harrison Barnes',
                '1629628': 'RJ Barrett',
                '203115': 'Will Barton',
                '1628966': 'Keita Bates-Diop',
                '201587': 'Nicolas Batum',
                '203382': 'Aron Baynes',
                '203145': 'Kent Bazemore',
                '1629647': 'Darius Bazley',
                '203078': 'Bradley Beal',
                '1627736': 'Malik Beasley',
                '1627761': "DeAndre' Bembry",
                '202722': 'Davis Bertans',
                '201976': 'Patrick Beverley',
                '1630180': 'Saddiq Bey',
                '1630189': 'Tyler Bey',
                '203920': 'Khem Birch',
                '1629048': 'Goga Bitadze',
                '202687': 'Bismack Biyombo',
                '202357': 'Nemanja Bjelica',
                '202339': 'Eric Bledsoe',
                '1629833': 'Keljin Blevins',
                '203992': 'Bogdan Bogdanovic',
                '202711': 'Bojan Bogdanovic',
                '1629626': 'Bol Bol',
                '1629067': 'Isaac Bonga',
                '1626164': 'Devin Booker',
                '1628449': 'Chris Boucher',
                '1628968': 'Brian Bowen II',
                '202340': 'Avery Bradley',
                '1628396': 'Tony Bradley',
                '1629714': 'Jarrell Brantley',
                '1629649': 'Ignas Brazdeikis',
                '1628969': 'Mikal Bridges',
                '1628970': 'Miles Bridges',
                '1627763': 'Malcolm Brogdon',
                '1628415': 'Dillon Brooks',
                '1628971': 'Bruce Brown',
                '1627759': 'Jaylen Brown',
                '1629650': 'Moses Brown',
                '1628425': 'Sterling Brown',
                '1628972': 'Troy Brown Jr',
                '1628973': 'Jalen Brunson',
                '1628418': 'Thomas Bryant',
                '203493': 'Reggie Bullock',
                '203504': 'Trey Burke',
                '202692': 'Alec Burks',
                '202710': 'Jimmy Butler',
                '1629719': 'Devontae Cacok',
                '203484': 'Kentavious Caldwell-Pope',
                '1630267': 'Facundo Campazzo',
                '1628427': 'Vlatko Cancar',
                '203991': 'Clint Capela',
                '1630176': 'Vernon Carey Jr',
                '1628975': 'Jevon Carter',
                '1628976': 'Wendell Carter Jr',
                '203487': 'Michael Carter-Williams',
                '1627936': 'Alex Caruso',
                '1626161': 'Willie Cauley-Stein',
                '1629185': 'Chris Chiozza',
                '1627737': 'Marquese Chriss',
                '1629109': 'Gary Clark',
                '1629634': 'Brandon Clarke',
                '203903': 'Jordan Clarkson',
                '1629651': 'Nicolas Claxton',
                '1629599': 'Amir Coffey',
                '1628381': 'John Collins',
                '1628380': 'Zach Collins',
                '201144': 'Mike Conley',
                '1626192': 'Pat Connaughton',
                '1629076': 'Tyler Cook',
                '203496': 'Robert Covington',
                '1628470': 'Torrey Craig',
                '203109': 'Jae Crowder',
                '1629633': 'Jarrett Culver',
                '203552': 'Seth Curry',
                '201939': 'Stephen Curry',
                '1630268': 'Nate Darling',
                '203076': 'Anthony Davis',
                '202334': 'Ed Davis',
                '1629056': 'Terence Davis',
                '203521': 'Matthew Dellavedova',
                '201942': 'DeMar DeRozan',
                '1629603': 'Mamadi Diakite',
                '1628977': 'Hamidou Diallo',
                '203476': 'Gorgui Dieng',
                '203915': 'Spencer Dinwiddie',
                '1628978': 'Donte DiVincenzo',
                '1629029': 'Luka Doncic',
                '1629652': 'Luguentz Dort',
                '1628422': 'Damyean Dotson',
                '1629653': 'Devon Dotson',
                '1629635': 'Sekou Doumbouya',
                '1628408': 'PJ Dozier',
                '201609': 'Goran Dragic',
                '203083': 'Andre Drummond',
                '201162': 'Jared Dudley',
                '1627739': 'Kris Dunn',
                '201142': 'Kevin Durant',
                '1630162': 'Anthony Edwards',
                '1629035': 'Carsen Edwards',
                '1629604': 'CJ Elleby',
                '201961': 'Wayne Ellington',
                '203954': 'Joel Embiid',
                '203516': 'James Ennis III',
                '1629234': 'Drew Eubanks',
                '203957': 'Dante Exum',
                '1629605': 'Tacko Fall',
                '202324': 'Derrick Favors',
                '1626245': 'Cristiano Felicio',
                '1628390': 'Terrance Ferguson',
                '1628981': 'Bruno Fernando',
                '1627827': 'Dorian Finney-Smith',
                '1630201': 'Malachi Flynn',
                '1627854': 'Bryn Forbes',
                '1630235': 'Trent Forrest',
                '203095': 'Evan Fournier',
                '1628368': "De'Aaron Fox",
                '1628365': 'Markelle Fultz',
                '1629117': 'Wenyen Gabriel',
                '1629655': 'Daniel Gafford',
                '201568': 'Danilo Gallinari',
                '204038': 'Langston Galloway',
                '1629636': 'Darius Garland',
                '201188': 'Marc Gasol',
                '200752': 'Rudy Gay',
                '202331': 'Paul George',
                '201959': 'Taj Gibson',
                '1628385': 'Harry Giles III',
                '1628983': 'Shai Gilgeous-Alexander',
                '1630264': 'Anthony Gill',
                '203497': 'Rudy Gobert',
                '1629164': 'Brandon Goodwin',
                '203932': 'Aaron Gordon',
                '201569': 'Eric Gordon',
                '1628984': "Devonte' Graham",
                '203924': 'Jerami Grant',
                '201980': 'Danny Green',
                '203110': 'Draymond Green',
                '203210': 'JaMychal Green',
                '1629750': 'Javonte Green',
                '201145': 'Jeff Green',
                '1630182': 'Josh Green',
                '201933': 'Blake Griffin',
                '1629657': 'Kyle Guy',
                '1629060': 'Rui Hachimura',
                '1630169': 'Tyrese Haliburton',
                '1629743': 'Donta Hall',
                '1630221': 'Josh Hall',
                '1630181': 'RJ Hampton',
                '203501': 'Tim Hardaway Jr',
                '201935': 'James Harden',
                '203090': 'Maurice Harkless',
                '1629607': 'Jared Harper',
                '1626149': 'Montrezl Harrell',
                '203914': 'Gary Harris',
                '1630223': 'Jalen Harris',
                '203925': 'Joe Harris',
                '202699': 'Tobias Harris',
                '1628404': 'Josh Hart',
                '1628392': 'Isaiah Hartenstein',
                '2617': 'Udonis Haslem',
                '1629637': 'Jaxson Hayes',
                '1630165': 'Killian Hayes',
                '202330': 'Gordon Hayward',
                '1627823': 'Juancho Hernangomez',
                '1626195': 'Willy Hernangomez',
                '1629639': 'Tyler Herro',
                '1627741': 'Buddy Hield',
                '201588': 'George Hill',
                '203524': 'Solomon Hill',
                '1630207': 'Nate Hinton',
                '1628988': 'Aaron Holiday',
                '201950': 'Jrue Holiday',
                '203200': 'Justin Holiday',
                '1626158': 'Richaun Holmes',
                '203918': 'Rodney Hood',
                '201143': 'Al Horford',
                '1629659': 'Talen Horton-Tucker',
                '1627863': 'Danuel House Jr',
                '2730': 'Dwight Howard',
                '1630210': 'Markus Howard',
                '1628989': 'Kevin Huerter',
                '1630190': 'Elijah Hughes',
                '1629631': "De'Andre Hunter",
                '1628990': 'Chandler Hutchison',
                '201586': 'Serge Ibaka',
                '2738': 'Andre Iguodala',
                '204060': 'Joe Ingles',
                '1627742': 'Brandon Ingram',
                '202681': 'Kyrie Irving',
                '1628371': 'Jonathan Isaac',
                '1628411': 'Wes Iwundu',
                '1628402': 'Frank Jackson',
                '1628367': 'Josh Jackson',
                '1628382': 'Justin Jackson',
                '202704': 'Reggie Jackson',
                '1628991': 'Jaren Jackson Jr',
                '1629713': 'Justin James',
                '2544': 'LeBron James',
                '1629610': 'DaQuan Jeffries',
                '1629660': 'Ty Jerome',
                '1630198': 'Isaiah Joe',
                '1629661': 'Cameron Johnson',
                '201949': 'James Johnson',
                '1629640': 'Keldon Johnson',
                '1626169': 'Stanley Johnson',
                '204020': 'Tyler Johnson',
                '203999': 'Nikola Jokic',
                '1627745': 'Damian Jones',
                '1630222': 'Mason Jones',
                '1630200': 'Tre Jones',
                '1626145': 'Tyus Jones',
                '1627884': 'Derrick Jones Jr',
                '201599': 'DeAndre Jordan',
                '202709': 'Cory Joseph',
                '1629662': 'Mfiondu Kabengele',
                '1626163': 'Frank Kaminsky',
                '202683': 'Enes Kanter',
                '1628379': 'Luke Kennard',
                '1628467': 'Maxi Kleber',
                '1630233': 'Nathan Knight',
                '1628995': 'Kevin Knox II',
                '1629723': 'John Konchar',
                '1627788': 'Furkan Korkmaz',
                '1628436': 'Luke Kornet',
                '1629066': 'Rodions Kurucs',
                '1628398': 'Kyle Kuzma',
                '203087': 'Jeremy Lamb',
                '1629641': 'Romeo Langford',
                '203897': 'Zach LaVine',
                '1627774': 'Jake Layman',
                '1629665': 'Jalen Lecque',
                '1627814': 'Damion Lee',
                '1630240': 'Saben Lee',
                '203458': 'Alex Len',
                '202695': 'Kawhi Leonard',
                '203086': 'Meyers Leonard',
                '1627747': 'Caris LeVert',
                '1630184': 'Kira Lewis Jr',
                '203081': 'Damian Lillard',
                '1629642': 'Nassir Little',
                '1626172': 'Kevon Looney',
                '201572': 'Brook Lopez',
                '201577': 'Robin Lopez',
                '201567': 'Kevin Love',
                '200768': 'Kyle Lowry',
                '1627789': 'Timothe Luwawu-Cabarrot',
                '1626168': 'Trey Lyles',
                '1630266': 'Will Magnay',
                '1630177': 'Theo Maledon',
                '1630211': 'Karim Mane',
                '1629611': 'Terance Mann',
                '1630185': 'Nico Mannion',
                '1626246': 'Boban Marjanovic',
                '1628374': 'Lauri Markkanen',
                '1630230': 'Naji Marshall',
                '1628997': 'Caleb Martin',
                '1628998': 'Cody Martin',
                '1629103': 'Kelan Martin',
                '1630231': 'Kenyon Martin Jr',
                '1629726': 'Garrison Mathews',
                '202083': 'Wesley Matthews',
                '1630178': 'Tyrese Maxey',
                '1630219': 'Skylar Mays',
                '1627775': 'Patrick McCaw',
                '203468': 'CJ McCollum',
                '204456': 'TJ McConnell',
                '1630183': 'Jaden McDaniels',
                '1629667': 'Jalen McDaniels',
                '203926': 'Doug McDermott',
                '1630253': 'Sean McDermott',
                '201580': 'JaVale McGee',
                '203585': 'Rodney McGruder',
                '1628035': 'Alfonzo McKinnie',
                '1629162': 'Jordan McLaughlin',
                '203463': 'Ben McLemore',
                '1629740': 'Nicolo Melli',
                '1629001': "De'Anthony Melton",
                '1630241': 'Sam Merrill',
                '1629002': 'Chimezie Metu',
                '203114': 'Khris Middleton',
                '203121': 'Darius Miller',
                '201988': 'Patty Mills',
                '200794': 'Paul Millsap',
                '1629003': 'Shake Milton',
                '1628378': 'Donovan Mitchell',
                '1629690': 'Adam Mokoka',
                '1628370': 'Malik Monk',
                '202734': "E'Twaun Moore",
                '1629630': 'Ja Morant',
                '1629752': 'Juwan Morgan',
                '202693': 'Markieff Morris',
                '1628420': 'Monte Morris',
                '202694': 'Marcus Morris Sr',
                '1628539': 'Mychal Mulder',
                '1627749': 'Dejounte Murray',
                '1627750': 'Jamal Murray',
                '203488': 'Mike Muscala',
                '1629004': 'Svi Mykhailiuk',
                '1627846': 'Abdel Nader',
                '1626204': 'Larry Nance Jr',
                '1630174': 'Aaron Nesmith',
                '203526': 'Raul Neto',
                '1627777': 'Georges Niang',
                '1630192': 'Zeke Nnaji',
                '203457': 'Nerlens Noel',
                '1629669': 'Jaylen Nowell',
                '1628373': 'Frank Ntilikina',
                '1629134': 'Kendrick Nunn',
                '203994': 'Jusuf Nurkic',
                '1628021': 'David Nwaba',
                '1629670': 'Jordan Nwora',
                '1626220': "Royce O'Neale",
                '1628400': 'Semi Ojeleye',
                '1626143': 'Jahlil Okafor',
                '1629643': 'Chuma Okeke',
                '1629006': 'Josh Okogie',
                '1630168': 'Onyeka Okongwu',
                '1630171': 'Isaac Okoro',
                '1629644': 'KZ Okpala',
                '203506': 'Victor Oladipo',
                '203482': 'Kelly Olynyk',
                '1629671': 'Miye Oni',
                '1626224': 'Cedi Osman',
                '1630187': 'Daniel Oturu',
                '1626162': 'Kelly Oubre Jr',
                '203953': 'Jabari Parker',
                '1629672': 'Eric Paschall',
                '202335': 'Patrick Patterson',
                '1628383': 'Justin Patton',
                '101108': 'Chris Paul',
                '1626166': 'Cameron Payne',
                '203901': 'Elfrid Payton',
                '203658': 'Norvel Pelle',
                '1629617': 'Reggie Perry',
                '1629033': 'Theo Pinson',
                '203486': 'Mason Plumlee',
                '1627751': 'Jakob Poeltl',
                '1629738': 'Vincent Poirier',
                '1630197': 'Aleksej Pokusevski',
                '1629673': 'Jordan Poole',
                '1629007': 'Jontay Porter',
                '1629645': 'Kevin Porter Jr',
                '1629008': 'Michael Porter Jr',
                '203490': 'Otto Porter Jr',
                '1626171': 'Bobby Portis',
                '204001': 'Kristaps Porzingis',
                '203939': 'Dwight Powell',
                '1626181': 'Norman Powell',
                '1627752': 'Taurean Prince',
                '1630202': 'Payton Pritchard',
                '1630193': 'Immanuel Quickley',
                '1630186': "Jahmi'us Ramsey",
                '1626184': 'Chasson Randle',
                '203944': 'Julius Randle',
                '1629629': 'Cam Reddish',
                '200755': 'JJ Redick',
                '1630194': 'Paul Reed',
                '1629675': 'Naz Reid',
                '1630208': 'Nick Richards',
                '1626196': 'Josh Richardson',
                '1630203': 'Grant Riller',
                '203085': 'Austin Rivers',
                '203460': 'Andre Roberson',
                '1629130': 'Duncan Robinson',
                '1629010': 'Jerome Robinson',
                '1629011': 'Mitchell Robinson',
                '1629676': 'Isaiah Roby',
                '200765': 'Rajon Rondo',
                '201565': 'Derrick Rose',
                '203082': 'Terrence Ross',
                '1626179': 'Terry Rozier',
                '201937': 'Ricky Rubio',
                '1626156': "D'Angelo Russell",
                '1627734': 'Domantas Sabonis',
                '1629677': 'Luka Samanic',
                '203960': 'JaKarr Sampson',
                '203967': 'Dario Saric',
                '203107': 'Tomas Satoransky',
                '203471': 'Dennis Schroder',
                '203118': 'Mike Scott',
                '1630206': 'Jay Scrubb',
                '1629012': 'Collin Sexton',
                '1629013': 'Landry Shamet',
                '202697': 'Iman Shumpert',
                '1627783': 'Pascal Siakam',
                '1629735': 'Chris Silva',
                '1627732': 'Ben Simmons',
                '1629014': 'Anfernee Simons',
                '1629686': 'Deividas Sirvydis',
                '1629346': 'Alen Smailagic',
                '203935': 'Marcus Smart',
                '202397': 'Ish Smith',
                '1630188': 'Jalen Smith',
                '1628372': 'Dennis Smith Jr',
                '203503': 'Tony Snell',
                '1630199': 'Cassius Stanley',
                '1630205': 'Lamar Stevens',
                '1630191': 'Isaiah Stewart',
                '1629622': 'Max Strus',
                '1628410': 'Edmond Sumner',
                '1630256': "Jae'Sean Tate",
                '1628369': 'Jayson Tatum',
                '201952': 'Jeff Teague',
                '202066': 'Garrett Temple',
                '1630179': 'Tyrell Terry',
                '1628464': 'Daniel Theis',
                '1630271': 'Brodric Thomas',
                '1629744': 'Matt Thomas',
                '202691': 'Klay Thompson',
                '202684': 'Tristan Thompson',
                '1628414': 'Sindarius Thornwell',
                '1629680': 'Matisse Thybulle',
                '1629681': 'Killian Tillie',
                '1630214': 'Xavier Tillman',
                '1630167': 'Obi Toppin',
                '1629308': 'Juan Toscano-Anderson',
                '1626157': 'Karl-Anthony Towns',
                '1629018': 'Gary Trent Jr',
                '200782': 'PJ Tucker',
                '1629730': 'Rayjon Tucker',
                '1626167': 'Myles Turner',
                '202685': 'Jonas Valanciunas',
                '1627756': 'Denzel Valentine',
                '1629020': 'Jarred Vanderbilt',
                '1627832': 'Fred VanVleet',
                '1630170': 'Devin Vassell',
                '1629216': 'Gabe Vincent',
                '202696': 'Nikola Vucevic',
                '1629731': 'Dean Wade',
                '1629021': 'Moritz Wagner',
                '202689': 'Kemba Walker',
                '1629022': 'Lonnie Walker IV',
                '202322': 'John Wall',
                '202954': 'Brad Wanamaker',
                '203933': 'TJ Warren',
                '1629023': 'PJ Washington',
                '1629139': 'Yuta Watanabe',
                '1629682': 'Tremont Waters',
                '1628778': 'Paul Watson',
                '1629683': 'Quinndary Weatherspoon',
                '201566': 'Russell Westbrook',
                '1629632': 'Coby White',
                '1628401': 'Derrick White',
                '202355': 'Hassan Whiteside',
                '204222': 'Greg Whittington',
                '203952': 'Andrew Wiggins',
                '1629684': 'Grant Williams',
                '1629026': 'Kenrich Williams',
                '101150': 'Lou Williams',
                '1630172': 'Patrick Williams',
                '1629057': 'Robert Williams III',
                '1629627': 'Zion Williamson',
                '1628391': 'DJ Wilson',
                '1629685': 'Dylan Windler',
                '1626159': 'Justise Winslow',
                '1630216': 'Cassius Winston',
                '1630164': 'James Wiseman',
                '1626174': 'Christian Wood',
                '1630218': 'Robert Woodard II',
                '1626153': 'Delon Wright',
                '201152': 'Thaddeus Young',
                '1629027': 'Trae Young',
                '203469': 'Cody Zeller',
                '1627826': 'Ivica Zubac'}

    #pbp: "/prod/v1/{{gameDate}}/{{gameId}}_pbp_{{periodNum}}.json"
    #currentScoreboard: "/prod/v1/20210224/scoreboard.json"
    #boxscore: "/prod/v1/{{gameDate}}/{{gameId}}_boxscore.json"
    def pkey(row):
        if row['selection'] != 'V':
            try:
                return (str(row['date']) + str(row['event']) + str(row['market']) + str(row['selection'])).replace(' ','')
            except TypeError:
                return 'L'
        else: return 'V'

    def get_boxscore_results():
        IDs = []
        url = f'http://data.nba.net/10s/prod/v1/{NBAdate}/scoreboard.json'
        r = requests.get(url)
        games = r.json()['games']
        for g in games:
            gID = g['gameId']
            IDs.append(gID)

        results = []
        for _,i in enumerate(IDs):
            time.sleep(1)
            url = f'http://data.nba.net/10s/prod/v1/{NBAdate}/{i}_boxscore.json'
            r = requests.get(url)
            payload = r.json()
            try:
                stats = payload['stats']
            except KeyError:
                continue
            event = payload['basicGameData']
            homeID = event['hTeam']['teamId']
            awayID = event['vTeam']['teamId']
            home = nbaTeams.get(homeID)
            away = nbaTeams.get(awayID)
            event_name = f'{home} v {away}'

            sys.stdout.write(f'Scraping {event_name}, event {_+1} of {len(IDs)}' + '\n')
            sys.stdout.flush()

            if event['period']['current'] > 4:
                isOT = 1
            else: isOT = 0

            homeFinal = int(event['hTeam']['score'])
            homeQ1 = int(event['hTeam']['linescore'][0]['score'])
            homeQ2 = int(event['hTeam']['linescore'][1]['score'])
            home1H = int(homeQ1) + int(homeQ2)
            homeQ3 = int(event['hTeam']['linescore'][2]['score'])
            homeQ4 = int(event['hTeam']['linescore'][3]['score'])

            awayFinal = int(event['vTeam']['score'])
            awayQ1 = int(event['vTeam']['linescore'][0]['score'])
            awayQ2 = int(event['vTeam']['linescore'][1]['score'])
            away1H = int(awayQ1) + int(awayQ2)
            awayQ3 = int(event['vTeam']['linescore'][2]['score'])
            awayQ4 = int(event['vTeam']['linescore'][3]['score'])

            # Head To Head
            if homeFinal > awayFinal:
                h2h = home
            else: h2h = away

            # Winning Team & Margin
            if h2h == home:
                winner = home
                margin = homeFinal - awayFinal
            else:
                winner = away
                margin = awayFinal - homeFinal
            if margin <= 10:
                margin = '1-10'
            else: margin = '11+'
            wtam = f'{winner} {margin}'

            # 1st Half Winner
            if home1H > away1H:
                h1winnner = home
            elif away1H > home1H:
                h1winnner = away
            else: h1winnner = 'V'

            # 1st Quarter Winner
            if homeQ1 > awayQ1:
                q1winner = home
            elif awayQ1 > homeQ1:
                q1winner = away
            else: q1winner = 'V'

            # 2nd Quarter Winner
            if homeQ2 > awayQ2:
                q2winner = home
            elif awayQ2 > homeQ2:
                q2winner = away
            else: q2winner = 'V'

            # 3rd Quarter Winner
            if homeQ3 > awayQ3:
                q3winner = home
            elif awayQ3 > homeQ3:
                q3winner = away
            else: q3winner = 'V'

            # 4th Quarter Winner
            if homeQ4 > awayQ4:
                q4winner = home
            elif awayQ4 > homeQ4:
                q4winner = away
            else: q4winner = 'V'

            # Will There Be Overtime?
            if isOT == 1:
                ot = 'Yes'
            else: ot = 'No'

            # Both Teams to Score 100+ Points
            if (homeFinal >= 100) and (awayFinal >= 100):
                btts100 = 'Yes'
            else: btts100 = 'No'

            # Both Teams to Score 110+ Points
            if (homeFinal >= 110) and (awayFinal >= 110):
                btts110 = 'Yes'
            else: btts110 = 'No'

            # Both Teams to Score 120+ Points
            if (homeFinal >= 120) and (awayFinal >= 120):
                btts120 = 'Yes'
            else: btts120 = 'No'

            # Team With Highest Scoring Quarter
            home_max = max([homeQ1,homeQ2,homeQ3,homeQ4])
            away_max = max([awayQ1,awayQ2,awayQ3,awayQ4])
            if home_max > away_max:
                thsq = home
            else: thsq = away

            # Top Points Scorer
            home_top = stats['hTeam']['leaders']['points']['value']
            away_top = stats['vTeam']['leaders']['points']['value']
            if home_top > away_top:
                player = stats['hTeam']['leaders']['points']['players']
            elif home_top < away_top:
                player = stats['vTeam']['leaders']['points']['players']
            elif home_top == away_top:
                player = None
                tps = []
                home_leaders = stats['hTeam']['leaders']['points']['players']
                away_leaders = stats['vTeam']['leaders']['points']['players']
                for l in home_leaders:
                    fName = l['firstName']
                    lName = l['lastName']
                    name = f'{fName} {lName}'.replace('.','')
                    tps.append(name)
                for l in away_leaders:
                    fName = l['firstName']
                    lName = l['lastName']
                    name = f'{fName} {lName}'.replace('.','')
                    tps.append(name)
            if player is None:
                tps = tps
            elif len(player) > 1:
                tps = []
                for p in player:
                    fName = p['firstName']
                    lName = p['lastName']
                    name = f'{fName} {lName}'.replace('.','')
                    tps.append(name)
            elif len(player) == 1:
                fName = player[0]['firstName']
                lName = player[0]['lastName']
                tps = f'{fName} {lName}'.replace('.','')

            # To Score X Points, X Rebounds, X Assists, X Steals, X Blocks, X Three Pt FGs, Double Double, Triple Double
            pts10, pts15, pts20, pts25, pts30, pts35, pts40, pts45, pts50 = [], [], [], [], [], [], [], [], []
            reb4, reb6, reb8, reb10, reb12, reb14, reb16, reb18, reb20 = [], [], [], [], [], [], [], [], []
            ast4, ast6, ast8, ast10, ast12, ast14 = [], [], [], [], [], []
            stl1, stl2, stl3, stl4, stl5 = [], [], [], [], []
            blk1, blk2, blk3, blk4, blk5 = [], [], [], [], []
            tpm1, tpm2, tpm3, tpm4, tpm5 = [], [], [], [], []
            dubdub, tripdub = [], []

            player_stats = stats['activePlayers']
            for p in player_stats:
                if p['min'] == '0:00':
                    fName = p['firstName']
                    lName = p['lastName']
                    name = f'{fName} {lName}'.replace('.','')
                    void_players.append(name)
                elif (p['min'] != '0:00') and (p['dnp'] == ''):
                    fName = p['firstName']
                    lName = p['lastName']
                    name = f'{fName} {lName}'.replace('.','')
                    pts = int(p['points'])
                    reb = int(p['totReb'])
                    ast = int(p['assists'])
                    stl = int(p['steals'])
                    blk = int(p['blocks'])
                    tpm = int(p['tpm'])
                    line = [pts,reb,ast,stl,blk]

                    if pts >=  10:
                        pts10.append(name)
                        if pts >=  15:
                            pts15.append(name)
                            if pts >=  20:
                                pts20.append(name)
                                if pts >=  25:
                                    pts25.append(name)
                                    if pts >=  30:
                                        pts30.append(name)
                                        if pts >=  35:
                                            pts35.append(name)
                                            if pts >=  40:
                                                pts40.append(name)
                                                if pts >=  45:
                                                    pts45.append(name)
                                                    if pts >=  50:
                                                        pts50.append(name)
                    if reb >=  4:
                        reb4.append(name)
                        if reb >=  6:
                            reb6.append(name)
                            if reb >=  8:
                                reb8.append(name)
                                if reb >=  10:
                                    reb10.append(name)
                                    if reb >=  12:
                                        reb12.append(name)
                                        if reb >=  14:
                                            reb14.append(name)
                                            if reb >=  16:
                                                reb16.append(name)
                                                if reb >=  18:
                                                    reb18.append(name)
                                                    if reb >=  20:
                                                        reb20.append(name)
                    if ast >=  4:
                        ast4.append(name)
                        if ast >=  6:
                            ast6.append(name)
                            if ast >=  8:
                                ast8.append(name)
                                if ast >=  10:
                                    ast10.append(name)
                                    if ast >=  12:
                                        ast12.append(name)
                                        if ast >=  14:
                                            ast14.append(name)
                    if stl >=  1:
                        stl1.append(name)
                        if stl >=  2:
                            stl2.append(name)
                            if stl >=  3:
                                stl3.append(name)
                                if stl >=  4:
                                    stl4.append(name)
                                    if stl >=  5:
                                        stl5.append(name)
                    if blk >=  1:
                        blk1.append(name)
                        if blk >=  2:
                            blk2.append(name)
                            if blk >=  3:
                                blk3.append(name)
                                if blk >=  4:
                                    blk4.append(name)
                                    if blk >=  5:
                                        blk5.append(name)
                    if tpm >=  1:
                        tpm1.append(name)
                        if tpm >=  2:
                            tpm2.append(name)
                            if tpm >=  3:
                                tpm3.append(name)
                                if tpm >=  4:
                                    tpm4.append(name)
                                    if tpm >=  5:
                                        tpm5.append(name)

                    dub = [x for x in line if len(str(x)) > 1]
                    if len(dub) >= 2:
                        dubdub.append(name)
                    trip = [x for x in line if len(str(x)) > 1]
                    if len(trip) >= 3:
                        tripdub.append(name)

            results.append([today,event_name,h2h,wtam,h1winnner,q1winner,q2winner,q3winner,q4winner,ot,btts100,btts110,btts120,thsq,tps,
            pts10, pts15, pts20, pts25, pts30, pts35, pts40, pts45, pts50,reb4, reb6, reb8, reb10, reb12, reb14, reb16, reb18, reb20,
            ast4, ast6, ast8, ast10, ast12, ast14, stl1, stl2, stl3, stl4, stl5, blk1, blk2, blk3, blk4, blk5, tpm1, tpm2, tpm3, tpm4, tpm5, dubdub, tripdub])

        columns = ['date','event','Head To Head','Winning Team & Margin','1st Half Winner','1st Quarter Winner','2nd Quarter Winner',
                '3rd Quarter Winner','4th Quarter Winner','Will There Be Overtime?','Both Teams to Score 100+ Points',
                'Both Teams to Score 110+ Points','Both Teams to Score 120+ Points','Team With Highest Scoring Quarter','Top Points Scorer','To Score 10+ Points',
                'To Score 15+ Points','To Score 20+ Points','To Score 25+ Points','To Score 30+ Points','To Score 35+ Points','To Score 40+ Points','To Score 45+ Points',
                'To Score 50+ Points','To Record 4+ Rebounds','To Record 6+ Rebounds','To Record 8+ Rebounds','To Record 10+ Rebounds','To Record 12+ Rebounds',
                'To Record 14+ Rebounds','To Record 16+ Rebounds','To Record 18+ Rebounds','To Record 20+ Rebounds',
                'To Record 4+ Assists','To Record 6+ Assists','To Record 8+ Assists','To Record 10+ Assists','To Record 12+ Assists','To Record 14+ Assists','To Record 1+ Steals',
                'To Record 2+ Steals','To Record 3+ Steals','To Record 4+ Steals','To Record 5+ Steals','To Record 1+ Blocks','To Record 2+ Blocks','To Record 3+ Blocks',
                'To Record 4+ Blocks','To Record 5+ Blocks','To Make 1+ Three Point FG','To Make 2+ Three Point FG','To Make 3+ Three Point FG',
                'To Make 4+ Three Point FG','To Make 5+ Three Point FG','To Record A Double Double','To Record A Triple Double']
        df = pd.DataFrame(results,columns=columns)
        return df

    def get_pbp_results():
        IDs = []
        url = f'http://data.nba.net/10s/prod/v1/{NBAdate}/scoreboard.json'
        r = requests.get(url)
        games = r.json()['games']
        for g in games:
            gID = g['gameId']
            IDs.append(gID)

        pbp = []
        for _,i in enumerate(IDs):
            time.sleep(1)
            url = f'http://data.nba.net/10s/prod/v1/{NBAdate}/{i}_boxscore.json'
            r = requests.get(url)
            payload = r.json()
            try:
                stats = payload['stats']
            except KeyError:
                continue
            event = payload['basicGameData']
            homeID = event['hTeam']['teamId']
            awayID = event['vTeam']['teamId']
            home = nbaTeams.get(homeID)
            away = nbaTeams.get(awayID)
            event_name = f'{home} v {away}'

            sys.stdout.write(f'Scraping {event_name}, event {_+1} of {len(IDs)}'+ '\n')
            sys.stdout.flush()

            time.sleep(1)
            url = f'http://data.nba.net/10s/prod/v1/{NBAdate}/{i}_pbp_1.json'
            r = requests.get(url)
            plays = r.json()['plays']
            # First Basket Scorer & First Team To Score
            for p in plays:
                if p['eventMsgType'] in ['1','3']:
                    playerFB = nbaPlayers.get(p['personId'])
                    teamFB = nbaTeams.get(p['teamId'])
                    break

            # First Team Basket Scorer
            for p in plays:
                if p['hTeamScore'] == '1':
                    homeFTB = nbaPlayers.get(p['personId'])
                    break
                elif p['hTeamScore'] == '2':
                    homeFTB = nbaPlayers.get(p['personId'])
                    break
                elif p['hTeamScore'] == '3':
                    homeFTB = nbaPlayers.get(p['personId'])
                    break
            for p in plays:
                if p['vTeamScore'] == '1':
                    awayFTB = nbaPlayers.get(p['personId'])
                    break
                elif p['vTeamScore'] == '2':
                    awayFTB = nbaPlayers.get(p['personId'])
                    break
                elif p['vTeamScore'] == '3':
                    awayFTB = nbaPlayers.get(p['personId'])
                    break
            ftb = [homeFTB,awayFTB]

            # Race To 8 Points
            for p in plays:
                homeScore = int(p['hTeamScore'])
                awayScore = int(p['vTeamScore'])
                if homeScore >= 8 and awayScore < 8:
                    rt8 = nbaTeams.get(p['teamId'])
                    break
                elif homeScore < 8 and awayScore >= 8:
                    rt8 = nbaTeams.get(p['teamId'])
                    break

            # Race To 10 Points
            for p in plays:
                homeScore = int(p['hTeamScore'])
                awayScore = int(p['vTeamScore'])
                if homeScore >= 10 and awayScore < 10:
                    rt10 = nbaTeams.get(p['teamId'])
                    break
                elif homeScore < 10 and awayScore >= 10:
                    rt10 = nbaTeams.get(p['teamId'])
                    break

            # Race To 15 Points
            for p in plays:
                homeScore = int(p['hTeamScore'])
                awayScore = int(p['vTeamScore'])
                if homeScore >= 15 and awayScore < 15:
                    rt15 = nbaTeams.get(p['teamId'])
                    break
                elif homeScore < 15 and awayScore >= 15:
                    rt15 = nbaTeams.get(p['teamId'])
                    break

            # Race To 20 Points
            for p in plays:
                homeScore = int(p['hTeamScore'])
                awayScore = int(p['vTeamScore'])
                if homeScore >= 20 and awayScore < 20:
                    rt20 = nbaTeams.get(p['teamId'])
                    break
                elif homeScore < 20 and awayScore >= 20:
                    rt20 = nbaTeams.get(p['teamId'])
                    break

            # Race To 25 Points
            for p in plays:
                homeScore = int(p['hTeamScore'])
                awayScore = int(p['vTeamScore'])
                if (homeScore >= 25) or (awayScore >= 25):
                    if homeScore >= 25 and awayScore < 25:
                        rt25 = nbaTeams.get(p['teamId'])
                        break
                    if homeScore < 25 and awayScore >= 25:
                        rt25 = nbaTeams.get(p['teamId'])
                        break
                else:
                    url2ndqtr = f'http://data.nba.net/10s/prod/v1/{NBAdate}/{i}_pbp_2.json'
                    response = requests.get(url2ndqtr)
                    plays2ndqtr = response.json()['plays']
                    for p2 in plays2ndqtr:
                        homeScore = int(p2['hTeamScore'])
                        awayScore = int(p2['vTeamScore'])
                        if homeScore >= 25 and awayScore < 25:
                            rt25 = nbaTeams.get(p2['teamId'])
                            break
                        if homeScore < 25 and awayScore >= 25:
                            rt25 = nbaTeams.get(p2['teamId'])
                            break

            # Race To 30 Points
            for p in plays:
                homeScore = int(p['hTeamScore'])
                awayScore = int(p['vTeamScore'])
                if (homeScore >= 30) or (awayScore >= 30):
                    if homeScore >= 30 and awayScore < 30:
                        rt30 = nbaTeams.get(p['teamId'])
                        break
                    if homeScore < 30 and awayScore >= 30:
                        rt30 = nbaTeams.get(p['teamId'])
                        break
                else:
                    url2ndqtr = f'http://data.nba.net/10s/prod/v1/{NBAdate}/{i}_pbp_2.json'
                    response = requests.get(url2ndqtr)
                    plays2ndqtr = response.json()['plays']
                    for p2 in plays2ndqtr:
                        homeScore = int(p2['hTeamScore'])
                        awayScore = int(p2['vTeamScore'])
                        if homeScore >= 30 and awayScore < 30:
                            rt30 = nbaTeams.get(p2['teamId'])
                            break
                        if homeScore < 30 and awayScore >= 30:
                            rt30 = nbaTeams.get(p2['teamId'])
                            break

            # Last Team To Score
            time.sleep(1)
            otURL = f'http://data.nba.net/10s/prod/v1/{NBAdate}/{i}_boxscore.json'
            otResponse = requests.get(otURL)
            finalQtr = otResponse.json()['basicGameData']['period']['current']
            if finalQtr != 0:
                lttsURL = f'http://data.nba.net/10s/prod/v1/{NBAdate}/{i}_pbp_{finalQtr}.json'
                r = requests.get(lttsURL)
                plays = r.json()['plays']
                for p in reversed(plays):
                    if p['isScoreChange'] == True:
                        ltts = nbaTeams.get(p['teamId'])
                        break

            pbp.append([today,event_name,playerFB,teamFB,ftb,rt8,rt10,rt15,rt20,rt25,rt30,ltts])
        columns = ['date','event','First Basket','First Team To Score','First Team Basket Scorer','Race To 8 Points','Race To 10 Points','Race To 15 Points','Race To 20 Points','Race To 25 Points','Race To 30 Points','Last Team To Score']
        df = pd.DataFrame(pbp,columns=columns)
        return df

    print('Getting box score results')
    box_score = get_boxscore_results()
    box_score = pd.melt(box_score,id_vars=['date','event'], value_vars=boxscore_cols, var_name='market', value_name='selection').explode('selection')
    print('Getting play by play results')
    play_by_play = get_pbp_results()
    play_by_play = pd.melt(play_by_play,id_vars=['date','event'], value_vars=pbp_cols, var_name='market', value_name='selection').explode('selection')

    df = box_score.append(play_by_play)
    df['pkey'] = df.apply(lambda row: pkey(row), axis=1)
    return df

def get_odds():
    engine = create_engine('POSTGRESQL ADDRESS') ### Removed database info
    sql = f"SELECT * FROM nba_odds WHERE date = '{SQLdate}'"
    df = pd.read_sql_query(sql,con=engine)
    return df

def merger(odds,results):
    cols = ['date','home_team','away_team','market','selection','odds','result']
    def set_results(row):
        if (row['selection'] in void_players) or (row['pkey'] == 'V'):
            return 'V'
        else:
            return 'W'

    def drop_x(df):
        to_drop = [x for x in df if x.endswith('_x')]
        df.drop(to_drop, axis=1, inplace=True)

    def profit_loss(row):
        if row.result == 'W':
            return float(row.odds)-1.0
        elif row.result == 'L':
            return -1.0
        elif row.result == 'V':
            return 0


    results['result'] = results.apply(lambda row: set_results(row), axis=1)
    merged = pd.merge(odds,results[['result','pkey']], on='pkey', how='outer').dropna(subset=['date'])
    merged.result = merged.result.fillna('L')
    cleanup = pd.DataFrame(merged.event.str.split(' v ',1).tolist(),columns = ['home_team','away_team'])
    final = pd.concat([merged,cleanup], axis=1, join='inner')
    final = final[cols].reset_index(drop=True)
    final['profit_loss'] = final.apply(lambda row: profit_loss(row), axis=1)
    return final

odds = get_odds()
results = get_results()
df = merger(odds,results)
print('Merge Complete')

def pkey(row):
    try:
        return (str(row['date']) + row['home_team'] + row['away_team'] + row['market'] + row['selection']).replace(' ','')
    except: return 'error'
df['pkey'] = df.apply(lambda row: pkey(row), axis=1)
df = df[df.pkey != 'error']
print(len(df), 'rows complete')

print('Connecting to PostgreSQL')
# create heroku postgres connection
heroku_engine = create_engine('POSTGRESQL ADDRESS', pool_pre_ping=True)   ### Removed database info
conn = heroku_engine.connect()
meta = MetaData(bind=heroku_engine)
meta.reflect(bind=heroku_engine)
nba_results = meta.tables['POSTGRESQL TABLE NAME'] ### Removed database info

# upsert data to postgres
print('Upsert in progress')
insert_statement = insert(nba_results).values(df.to_dict(orient='records'))
upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=['pkey'],
        set_={c.key: c for c in insert_statement.excluded if c.key != 'pkey'})

# execute and close
conn.execute(upsert_statement)
conn.close()
print('Success')
