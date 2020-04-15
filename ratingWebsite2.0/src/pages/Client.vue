<template>
<div>
    <div class="q-pa-md">
        <div class="bg-primary text-white">
            <q-toolbar>
                <q-btn flat round dense icon="account_circle" style="font-size: 1.8em;" />
                <q-toolbar-title>User</q-toolbar-title>
                <logout/>
            </q-toolbar>
        </div>
    </div>

    <q-page class="q-pa-md q-gutter-lg">
     <q-card class="row flex flex-center">
      <q-card-section class="col ">
             <img alt="Quasar logo" src="~assets/roankee-steam.svg" style="width:70%; object-fit: contain">
      </q-card-section>
      <q-card-section class="col">
       <q-form @submit="onSubmit" class="q-gutter-md">
        <q-select
        name="developer"
        v-model="dev_sel"
        :options="dev_options"
        @filter="filter_dev"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="developer"
      />

        <q-select
        name="publisher"
        v-model="pub_sel"
        :options="pub_options"
        @filter="filter_pub"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="publisher"
      />

        <q-select
        name="platforms"
        v-model="plt_sel"
        multiple
        use-chips
        :options="plt_options"
        color="primary"
        filled
        clearable
        label="platforms"
      />

        <q-select
        name="categories"
        v-model="cat_sel"
        multiple
        use-chips
        :options="cat_options"
        @filter="filter_cat"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="categories"
      />

        <q-select
        name="tags"
        v-model="tag_sel"
        multiple
        use-chips
        :options="tag_options"
        @filter="filter_tag"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="tags"
      />
        <q-input outlined v-model="price" label="price"
        mask="#.##"
        fill-mask="0"
        reverse-fill-mask/>

        <p class="q-mt-md"></p>
        <div class="q-gutter-lg btn" style="text-align:center">
         <q-btn label="Submit" type="submit" color="primary"/>
         <q-btn @click="clear" label="clear" color="primary"/>
        </div>
       </q-form>
       <q-space />
      </q-card-section>
     </q-card>
    </q-page>

    <!-- <q-page class="flex flex-center">
     <q-form @submit="onSubmit" class="q-gutter-md">
      <q-select
        name="developer"
        v-model="dev_sel"
        :options="dev_options"
        @filter="filter_dev"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="developer"
      />

      <q-select
        name="publisher"
        v-model="pub_sel"
        :options="pub_options"
        @filter="filter_pub"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="publisher"
      />

      <q-select
        name="platforms"
        v-model="plt_sel"
        multiple
        use-chips
        :options="plt_options"
        color="primary"
        filled
        clearable
        label="platforms"
      />

      <q-select
        name="categories"
        v-model="cat_sel"
        multiple
        use-chips
        :options="cat_options"
        @filter="filter_cat"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="categories"
      />

      <q-select
        name="tags"
        v-model="tag_sel"
        multiple
        use-chips
        :options="tag_options"
        @filter="filter_tag"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="tags"
      />
      <q-input outlined v-model="price" label="price"
        mask="#.##"
        fill-mask="0"
        reverse-fill-mask/>

      <p class="q-mt-md"></p>
      <div class="q-gutter-lg btn" style="text-align:center">
         <q-btn label="Submit" type="submit" color="primary"/>
         <q-btn @click="clear" label="clear" color="primary"/>
      </div>
     </q-form>
    </q-page> -->

    <q-dialog v-model="isDialog">
      <q-card>
        <q-img src = "https://steamcdn-a.akamaihd.net/steam/apps/2840/header.jpg?t=1512663836"/>
        <q-card-section>
            <div class="row no-wrap items-center">
              <div class="col text-h6 ellipsis">
              Prediction result
              </div>
              <div class="col-auto ellipsis text-teal" style="font-size: 1.2em;">
                <q-icon name="done_outline "/>
                stars:{{label+1}}
              </div>
            </div>
            <div class="row no-wrap items-center">
                <div class="col">
                    <q-rating v-model="stars" :max="5" size="32px" />
                </div>
            </div>
          <!-- <q-rating v-model="stars" :max="5" size="32px" /> -->

        </q-card-section>
        <q-separator />
        <q-card-section>
            <div class="row no-wrap items-center">
            <div class="col text-h6 ellipsis">
              Accuracy
            </div>
            <div class="col-auto ellipsis text-teal" style="font-size: 1.2em;">
              <q-icon name="info "/>
              about
            </div>
          </div>
          <p>{{accuracy}}</p>
        </q-card-section>
        <q-separator />
        <q-card-section>
            <div class="row no-wrap items-center">
            <div class="col text-h6 ellipsis">
              Popularity Level
            </div>
            <div class="col-auto ellipsis text-teal" style="font-size: 1.2em;">
              <q-icon name="info "/>
              about
            </div>
          </div>
          <p> {{level}}</p>
        </q-card-section>
        <q-separator />
        <q-card-section class="q-pt-none">
          <div class="text-orange ellipsis" style="font-size: 1.0em;">
            Team9 production
          </div>
          <div class="text-caption text-grey">
            This prediction result is for reference only & all rights reserved.
          </div>
        </q-card-section>

        <q-card-actions align="right">
          <q-btn flat label="Got it" color="primary" v-close-popup />

        </q-card-actions>
      </q-card>
    </q-dialog>
</div>
</template>

<script>
import Logout from 'src/components/Logout.vue'
import axios from 'axios'

const dev = ['Feral Interactive (Mac)', 'Valve', 'Quiet River', 'Nihon Falcom', 'Artifex Mundi', '07th Expansion', 'VisualArts/Key', 'NEKO WORKs', 'Relic Entertainment', 'Scott Cawthon', 'Neko Climax Studios', 'Rusty Lake', 'Free Lives', 'Matt Roszak', 'GirlGame', 'Coffee Stain Studios', 'Walter Machado', 'Daedalic Entertainment', 'Zachtronics', 'Feral Interactive (Linux)', 'Frontwing', 'id Software', 'Wadjet Eye Games', 'Aspyr (Mac)', 'PopCap Games, Inc.', 'ALICE IN DISSONANCE', 'Tomorrow Corporation', 'CAVE Interactive CO.,LTD.', 'Image & Form Games', 'The Behemoth', 'PixelFade Inc', '@unepic_fran', 'Hopoo Games', 'Ironhide Game Studio', 'TreeFortress Games', 'Nickervision Studios', 'Vaka Game Magazine', 'Spike Chunsoft Co., Ltd.', 'BioWare', 'Infinity Ward', 'CAPCOM Co., Ltd.', 'Looking Glass Studios', 'Lose', 'InterAction studios', 'Lingtan Studio', 'Frictional Games', 'DrinkBox Studios', 'Suspicious Developments', 'Studio Pixel', 'Klei Entertainment', 'minori', 'Stainless Games Ltd', 'Konstructors Entertainment', 'Compile Heart', 'Double Fine Productions', 'Humongous Entertainment', 'Fireproof Games', '上海アリス幻樂団', 'Razzart Visual', 'Revolution Software Ltd', 'Crew Lab', 'Krome Studios', 'Harvester Games', 'Rabbiton', 'KillHouse Games', 'Terry Cavanagh', 'CreSpirit', 'What Pumpkin Games, Inc.', 'OhNoo Studio', 'Dominique Grieshofer', 'Studio Coattails', 'GrizzlyGames', 'Felistella', 'etherane', 'Novectacle', 'Ian Campbell', 'Silver Dollar Games', 'Subset Games', 'Claeysbrothers', 'Robot Entertainment', 'AKATSUKI-WORKS', 'Tendershoot', 'Pony', 'The Game Kitchen', 'Pirate Software', 'Thomas Bowker', 'Onyx Lute', 'Watercress Studios', 'Powerhoof', 'anchor Inc.', 'Daniel Mullins Games', 'Mango Protocol', 'Playdead', 'sen', 'Dennaton Games', 'Revival Productions, LLC', 'JCKSLAP', 'Lucas Pope', 'PALETTE', 'Andrew Morrish', 'kChamp Games', 'Resonair', 'FromSoftware, Inc', 'Aardman Animations', 'Battlecruiser Games', 'RageSquid', 'Feral Interactive (Mac/Linux)', 'Vectorpark', 'TAMSOFT', 'Timofey Shargorodskiy', 'Application Systems Heidelberg', 'COSEN', 'Nova-box', 'Light Bringer Games inc.', 'Sergey Dvoynikov', 'Ludosity', 'CD PROJEKT RED', 'Whirlpool', 'Amanita Design', 'InvertMouse', 'NukGames', 'Ithaqua Labs', 'SEGA', 'MoeNovel', 'Idea Factory', 'Innocent Grey', 'Supergiant Games', 'Michal Pawlowski', 'FRENCH-BREAD', 'Ska Studios', 'PlatinumGames', 'Virtual Programming', 'Runic Games', 'Team Reptile', 'Studio Élan', 'Ritual Entertainment', 'Alan Hazelden', 'Katauri Interactive', '致意', 'Hollowings', 'KADOKAWA', 'Capcom', 'Pilgrim Adventures', 'Winged Cloud', "Traveller's Tales", 'Croteam VR', 'Puppygames', 'Maciej Targoni', 'Hanako Games', 'Crankage Games', '凝冰剑斩', 'Bethesda Game Studios', 'Might and Delight', 'Fallen Tree Games Ltd', 'FobTi interactive', 'Capybara Games', 'Epic Games, Inc.', 'Fantasia Studio', 'Cloak and Dagger Games', '灰烬天国', 'Outcorp', 'Secret Location Inc.', 'Bedtime Digital Games', 'Twisted Pixel Games', 'Radial Games Corp', 'Sumo Digital', 'stage-nana', 'TaleWorlds Entertainment', 'KOGADO STUDIO', 'Blue Wizard Digital', 'MagicalTimeBean', '△○□× (Miwashiba)', 'Noumenon Games', 'Bugbear Entertainment', 'Design Factory', 'Love Conquers All Games', 'Heartbit Interactive', 'Crazy Monkey Studios', 'Iguana Entertainment', 'Crows Crows Crows', 'Jesse Makkonen', 'Arrowhead Game Studios', 'SUPERHOT Team', 'Proton Studio Inc', 'Spiderweb Software', 'WayForward', 'TT Games', 'Giants Software', 'Tamsoft', 'Piranha Bytes', 'DYA Games', 'MOONSTONE', 'Spicy Tails', 'Io-Interactive A/S', 'Blue Giraffe', 'Rocksteady Studios', 'Leslaw Sliwko', 'FreakZone Games', 'Firaxis Games', 'LucasArts', 'Rebellion', 'Zoom Out Games', 'Windybeard', 'Majorariatto', 'Nimbly Games', 'Magenta Factory', 'Llamasoft Ltd.', 'Zeno Rogue', 'Ripknot Systems', 'Ubisoft Montreal', 'Jetdogs Studios', 'Phil Fortier', 'Survios', 'Shiro Games', 'Lantis', 'Kodansha', 'Hidden Path Entertainment', 'NEXT Studios', 'Argent Games', 'The Irregular Corporation', 'Wojciech Wasiak', 'Freebird Games', 'Mojiken Studio', 'Bithell Games', 'Knuckle Cracker', 'Nival', 'Croteam', 'Two Tribes', 'Ensemble Studios', 'Illwinter Game Design', 'Matthew Brown', 'Trion Worlds', 'GrabTheGames Studios', 'Tripwire Interactive', 'Nippon Ichi Software, Inc.', 'Narrator', 'Core Design', 'Pendulo Studios', '11 bit studios', 'Google', 'Vlambeer', 'Tequila Works', 'Vanguard Games', 'Flying Wild Hog', 'AMPLITUDE Studios', 'Graviteam', 'DONTNOD Entertainment', 'Kittehface Software', 'Zetsubou', 'Stoic', 'DIMPS', 'Orange_Juice', 'FIVE-BN GAMES', '10tons Ltd', 'Arkane Studios', 'Tin Man Games', 'Obsidian Entertainment', 'Dexion Games', 'Raven Software', 'Laush Dmitriy Sergeevich', 'CREATIVE ASSEMBLY', 'Beamdog', 'inkle Ltd', 'Awesome Games Studio', 'Volition', 'Hijong Park', 'DigitalEZ', 'Owlchemy Labs', 'M2H', 'BottleCube inc.', 'Steve Gal', 'Stormregion', 'Coldwild Games', 'Thylacine Studios', 'Ninja Theory', 'Phaser Lock Interactive', 'Irrational Games', 'Tap It Games', 'Dancing Dragon Games', 'Uri Games', 'Alexey Davydov', 'Triumph Studios', 'Avalanche Studios', 'ASTRO PORT', 'Gearbox Software', 'Q-Games Ltd.', 'Hesketh Studios Ltd', 'Cardboard Computer', 'TT Games Ltd', 'ILMxLAB', 'Violet Feature', 'Dimps Corporation', 'Coilworks', 'THE BROTHERHOOD', "D'Avekki Studios Ltd", 'Connor O.R.T. Linning', 'Playful Corp.', 'ShiinaYashiro', 'Kidalang', 'Marina Kittaka', 'Vector Unit', 'Reflect Studios', 'Codename Entertainment Inc.', 'RLR Training Inc', 'the whale husband', 'Infamous Quests', 'SpielmannSpiel', 'Terri Vellmann', 'MyACG Studio', 'Robot Gentleman', 'Killerfish Games', 'Redblack Spade', 'Oscar Brittain', 'Hunchback Studio', 'Mikołaj Spychał', 'Lazy Monday Games', 'Digital Cybercherries', 'TIKIPOD', '内购人生PABL', 'Daniel Jonathan Bourke', 'Midgar Studio', 'Dylan Fitterer', 'Mehsoft', 'Four Circle Interactive', 'Freakinware Studios', 'Hassey Enterprises, Inc.', '4A Games', 'Blackmill Games', 'KirUn', 'kuklam studios', 'Lapovich', 'ESQUADRA,inc.', 'Crytek Studios', 'Elias Viglione', 'Organic 2 Digital Studio', 'light', 'Stray Fawn Studio', 'Numantian Games', 'Toxic Games', 'NIGORO', 'Dotoyou Games', 'Leiting Interactive', 'Lazy Bear Games', 'Curve Digital', 'Noble Master LLC', 'Dudarev Alexandr Vladimirovich', 'David Wehle', 'MrCiastku', 'STUDIO HG', 'ILLUSION', 'Capcom Vancouver', 'Computer Lunch', 'Flying Oak Games', 'BioWare Corporation', 'Skinny Jean Death Studios', 'disfactNoir', 'IndieGala', 'Benjamin Soulé', 'Ubisoft Annecy', 'GZ Storm', '3DClouds.it', 'StarWraith 3D Games LLC', 'Orchid Games', 'INSIDE SYSTEM', 'Circle Poison', 'Lighthouse Games Studio', 'Nerd Monkeys®', 'Solecismic Software', 'Gritfish', 'Incandescent Games', 'Onlyjoy`s production', 'Waterlily Games', 'Double Stallion Games', 'Still Running', 'Joakim Sandberg', "Arzola's", 'Green Dinosaur Games', 'Clifftop Games', 'Date Nighto', 'Kaigan Games OÜ', 'Cellar Door Games', 'Ubisoft - San Francisco', 'Dan Fornace', 'Benjamin Davis', 'Krillbite Studio', 'Going Loud Studios', 'Question', 'DeathMofuMofu', 'Press Play', 'Big Huge Games', 'Team Eleven', 'AZAMATIKA', 'Magic Notion Ltd', 'Kyle Seeley', 'Erik Asmussen', 'Remar Games', 'DEVGRU-P', 'LR Studio', 'Red Barrels', 'Infinite Dreams', 'Marvelous Inc.', 'Almost Human Games', 'Fred Wood', 'calme', 'INTI CREATES CO., LTD.', 'MAGES. Inc.', 'Eidos-Montréal', 'Cherry Pop Games', '一次元创作组', 'Fire Hose Games', 'Nightdive Studios', 'TALESSHOP Co., Ltd.', 'Liar-soft', 'SmashGames', 'SITER SKAIN', '343 Industries', 'Niffler Ltd.', 'Crackshell', 'Jundroo, LLC', 'Polygon Pictures Inc.', 'Ryu Productions Co., Ltd', 'Destructive Creations', 'Funktronic Labs', 'Oxeye Game Studio', 'Stranga', 'Hello There AB', 'TERNOX', 'ebi-hime', 'L. Stotch', 'ClickGames', 'VR Designs', 'Unknown Worlds Entertainment', 'Love in Space', 'Sierra Lee', 'BrainGoodGames', 'Milkstone Studios', 'Telltale Games', 'Team17 Digital Ltd', 'Crytek', 'Digital Leisure Inc.', 'Harebrained Schemes', 'Frozenbyte']
const pub = ['Sekai Project', 'SEGA', 'Devolver Digital', 'Feral Interactive (Mac)', 'MangaGamer', 'XSEED Games', 'Valve', 'Marvelous USA, Inc.', 'Quiet River', 'Spike Chunsoft Co., Ltd.', 'Wadjet Eye Games', 'HIKARI FIELD', 'Scott Cawthon', 'GirlGame', 'Neko Climax Studios', 'Rusty Lake', 'Matt Roszak', 'Walter Machado', 'Zachtronics', 'Frontwing USA', 'AGM PLAYISM', 'NVLMaker', 'Feral Interactive (Linux)', 'Big Fish Games', 'id Software', 'Bethesda Softworks', 'Electronic Arts', 'BANDAI NAMCO Entertainment', 'Idea Factory International', 'Daedalic Entertainment', 'Warner Bros. Interactive Entertainment', 'Ironhide Game Studio', 'PixelFade Inc', 'VisualArts', '@unepic_fran', 'The Behemoth', 'Image & Form Games', 'Tomorrow Corporation', 'TreeFortress Games', 'Ubisoft', 'Nickervision Studios', 'Aspyr (Mac)', 'Microsoft Studios', 'Vaka Game Magazine', 'Gamera Game', 'Lucasfilm', 'Suspicious Developments', 'Lingtan Studio', 'Hamster On Coke Games', 'DrinkBox Studios', 'InterAction studios', 'CAPCOM Co., Ltd.', 'Klei Entertainment', 'Ludosity', 'Coffee Stain Publishing', 'THQ Nordic', 'Square Enix', 'Marvelous', 'Mediascape Co., Ltd.', 'Fireproof Games', 'Razzart Visual', 'Humongous Entertainment', 'Konstructors Entertainment', 'LucasArts', 'Double Fine Productions', 'indienova', 'WhisperGames', 'Krome Studios', 'Crew Lab', 'Pony', 'Mango Protocol', 'GrizzlyGames', 'Revival Productions, LLC', 'etherane', 'The Hidden Levels', 'Onyx Lute', 'Bethesda-Softworks', 'Playdead', 'Daniel Mullins Games', 'Silver Dollar Games', 'Matt Makes Games Inc.', 'JAST USA\t', 'MBDL', 'Ian Campbell', 'Dominique Grieshofer', 'Robot Entertainment', 'Watercress Studios', 'What Pumpkin Games, Inc.', 'Magenta Factory', 'Pirate Software', 'KillHouse Games', 'Terry Cavanagh', 'Rabbiton', 'Powerhoof', 'Subset Games', 'OhNoo Studio', 'Idea Factory', '3909', 'Enhance', 'Battlecruiser Games', 'Nova-box', 'Llamasoft Ltd.', 'Light Bringer Games inc.', 'Aspyr (Mac, Linux)', 'Vectorpark', 'kChamp Games', 'Feral Interactive (Mac/Linux)', 'Those Awesome Guys', 'CD PROJEKT RED', 'Maciej Targoni', 'Degica', 'Frictional Games', 'Nightdive Studios', 'Amanita Design', 'Might and Delight', 'InvertMouse', 'Application Systems Heidelberg', 'GrabTheGames', 'NukGames', 'Revolution Software Ltd', 'Ithaqua Labs', 'PopCap Games, Inc.', 'tinyBuild', 'MoeNovel', 'Team Reptile', 'Ska Studios', 'AZAMATIKA', 'LR Studio', 'Supergiant Games', 'Runic Games', '致意', 'Draknek', '2K', 'Pujia8 Studio', 'Winged Cloud', 'Curve Digital', 'Activision', 'Crankage Games', '凝冰剑斩', 'Armor Games Studios', 'Paradise Project', 'Chess no choke games', 'Epic Games, Inc.', '灰烬天国', 'Cloak and Dagger Games', 'Bedtime Digital Games', 'Secret Location Inc.', 'Noumenon Games', 'Love Conquers All Games', 'TaleWorlds Entertainment', 'Heartbit Interactive', 'Crows Crows Crows', 'Kittehface Software', 'Crazy Monkey Studios', 'Jesse Makkonen', 'SUPERHOT Team', 'Nyu Media', 'Spiderweb Software', 'Adult Swim Games', 'Rebellion', 'Io-Interactive A/S', 'SmashGames', 'DYA Games', 'Age of Fear', 'Artifex Mundi', 'WayForward', 'Fellow Traveller', 'Fruitbat Factory', 'Dagestan Technology', 'Windybeard', 'Majorariatto', 'Nimbly Games', 'NekoNyan Ltd.', 'FromSoftware, Inc', 'Ripknot Systems', 'United Independent Entertainment GmbH', 'Phil Fortier', 'Shiro Games', 'Survios', 'Freebird Games', 'Annapurna Interactive', 'NEXT Studios', 'Argent Games', '迷糊的安安', 'Bithell Games', 'Two Tribes Publishing', 'Knuckle Cracker', 'Ghostlight LTD', 'Illwinter Game Design', 'Paradox Interactive', 'Matthew Brown', 'Puppygames', 'FIVE-BN GAMES', 'DANGEN Entertainment', 'Hanako Games', 'VT Publishing', 'Jetdogs Studios', 'Google', '1C-SoftClub', 'Ysbryd Games', 'Rockstar Games', 'Hound Picked Games', 'ClickGames', '北京网元圣唐娱乐科技有限公司', 'Cartoon Network Games', 'Edmund McMillen', 'GSC Game World', 'New Blood Interactive', 'Laush Studio', '10tons Ltd', 'Tripwire Interactive', 'Telltale Games', 'Tin Man Games', 'L. Stotch', 'Dexion Games', 'Giants Software', 'Croteam', 'Beamdog', 'Awesome Games Studio', 'inkle Ltd', '7DOTS', 'Pugware', 'M2H', 'Oasis Games', 'BottleCube inc.', 'Coldwild Games', 'INDIECN', 'FobTi interactive', 'Perfect World Entertainment', 'Overflow', 'Phaser Lock Interactive', 'Blue Wizard Digital', 'Thylacine Studios LLC', 'WhiteLakeStudio', 'Wizards of the Coast LLC', 'Flazm', 'Screenwave Media', 'KISS ltd', 'Trion Worlds', 'Frozenbyte', 'Humble Bundle', 'Good Shepherd Entertainment', 'Reflect Studios', 'Killerfish Games', 'Croteam Publishing', 'kuklam studios', 'Axolot Games', 'Freakinware Studios', 'Tozai Games, Inc.', '内购人生PABL']
const cat = ['Steam Achievements', 'Steam Trading Cards', 'Steam Cloud', 'Full controller support', 'Steam Leaderboards', 'Includes level editor', 'Steam Workshop', 'Captions available', 'Valve Anti-Cheat enabled', 'SteamVR Collectibles', 'Stats', 'Co-op', 'Single-player', 'Includes Source SDK', 'Multi-player', 'MMO', 'Shared/Split Screen', 'Commentary available', 'In-App Purchases', 'Cross-Platform Multiplayer', 'VR Support', 'Online Co-op', 'Online Multi-Player', 'Partial Controller Support', 'Local Co-op', 'Mods']
const tag = ['Indie', 'Action', 'Casual', 'Strategy', 'Simulation', 'Early Access', 'Puzzle', 'Platformer', 'Sexual Content', 'Visual Novel', 'Anime', 'Point & Click', 'FPS', 'Multiplayer', 'Pixel Graphics', 'Female Protagonist', 'Classic', 'Story Rich', 'Great Soundtrack', 'Co-op', 'JRPG', 'Comedy', 'Atmospheric', 'Cute', 'Dating Sim', 'Open World', 'Bullet Hell', 'Choices Matter', 'Difficult', 'Metroidvania', 'Rogue-like', 'Nudity', 'Hack and Slash', 'Massively Multiplayer', 'Post-apocalyptic', 'Funny', 'Local Multiplayer', 'Sci-fi', 'Stealth', '2D Fighter', 'Fighting', 'Horror', 'Cyberpunk', 'Memes', 'Adventure', 'LEGO', 'Programming', 'Character Action Game', 'Base-Building', 'RTS', 'First-Person', 'Sports', 'Strategy RPG', 'Parkour', 'Survival Horror', 'Fantasy', 'RPG', 'Mature', 'Free to Play', 'Cats', "Shoot 'Em Up", 'Mystery', 'Turn-Based Strategy', 'Choose Your Own Adventure', 'Rhythm', 'Faith', 'Villain Protagonist', 'Turn-Based', 'Military', 'Building', 'Character Customization', 'Steampunk', '1980s', 'Arcade', 'Action RPG', 'Multiple Endings', 'Western', 'Zombies', 'Hidden Object', 'Narration', 'Tower Defense', 'Exploration', 'Detective', 'Otome', 'Violent', '6DOF', '3D Platformer', 'Star Wars', 'Sandbox', 'Dark Fantasy', 'Psychological Horror', 'Local Co-Op', 'Dark Humor', 'Music', 'Mythology', 'Game Development', 'Fast-Paced', 'Assassin', 'Puzzle-Platformer', 'Rogue-lite', 'Snowboarding', 'Dungeons & Dragons', 'Linear', 'Web Publishing', 'Medieval', 'Noir', 'Racing', 'Split Screen', 'Trading Card Game', 'Quick-Time Events', 'Batman', 'Minimalist', 'Lovecraftian', 'Space', 'World War II', 'Replay Value', 'Grand Strategy', 'Card Game', 'FMV', 'Gore', 'Turn-Based Combat', 'Ninja', 'Survival', 'Remake', 'Management', 'Cartoony', 'Party-Based RPG', 'Abstract', 'Retro', 'Typing', 'Surreal', 'Superhero', "Beat 'em up", 'Robots', 'Supernatural', 'Thriller', 'Psychological', 'Cartoon', 'Historical', 'City Builder', 'Illuminati', 'Dungeon Crawler', 'Word Game', 'Mini Golf', 'Dystopian ', 'God Game', 'Hand-drawn', 'Sokoban']
const popularity = ['Negtive', 'Mostly Positive', 'Positive', 'Very Positive', 'Overhwelmingly Positive']

function initData () {
  return {
    // selection options
    dev_options: dev,
    pub_options: pub,
    plt_options: ['linux', 'mac', 'windows'],
    cat_options: cat,
    tag_options: tag,
    // input field
    price: 0,

    // to store selections data
    dev_sel: '',
    pub_sel: '',
    plt_sel: [],
    cat_sel: [],
    tag_sel: [],

    // response info
    info: '',
    label: 0,
    stars: 0,
    level: '',
    accuracy: 0,
    isDialog: false
  }
}

export default {
  name: 'Client',
  components: {
    Logout
  },

  data () {
    return initData()
  },

  methods: {
    onSubmit: function () {
      this.consolePrint()
      const inputdata = this.sendDataConstruct()
      console.log(inputdata)
      axios.post('http://127.0.0.1:5000/predict', inputdata)
        .then(response => {
          this.info = response.data
          this.parseInfo(this.info)
          //   this.stars = this.info
          this.isDialog = true
          console.log(this.info)
        })
        .catch(error => {
          alert('error:' + error.response.data)
        })
    },

    sendDataConstruct: function () {
      var param = {
        developer: this.dev_sel,
        publisher: this.pub_sel,
        platforms: this.parseData(this.plt_sel),
        categories: this.parseData(this.cat_sel),
        tags: this.parseData(this.tag_sel),
        price: this.price
      }
      return param
    },

    consolePrint: function () {
      console.log(this.dev_sel)
      console.log(this.pub_sel)
      console.log(this.plt_sel)
      console.log(this.cat_sel)
      console.log(this.tag_sel)
      console.log(this.price)
    },

    parseData: function (arr) {
      var text = ''
      for (var i = 0; i < arr.length; i++) {
        text += arr[i] + ';'
      }
      return text
    },

    parseInfo: function (data) {
      var arr = data.split(',')
      for (var i = 0; i < arr.length; i++) {
        this.label = parseInt(arr[0])
        this.stars = this.label + 1.0
        // this.accuracy = arr[1]
        this.accuracy = Number.parseFloat(arr[1]).toFixed(4)
      }
      switch (this.label) {
        case 0 :
          this.level = popularity[0]
          break
        case 1 :
          this.level = popularity[1]
          break
        case 2 :
          this.level = popularity[2]
          break
        case 3 :
          this.level = popularity[3]
          break
        case 4 :
          this.level = popularity[4]
          break
        default:
          this.level = ''
      }
    },

    clear: function () {
      Object.assign(this.$data, initData())
    },

    filter_dev (val, update) {
      if (val === '') {
        update(() => {
          this.dev_options = dev
        })
        return
      }
      update(() => {
        const needle = val.toLowerCase()
        this.dev_options = dev.filter(v => v.toLowerCase().indexOf(needle) > -1)
      })
    },

    filter_pub (val, update) {
      if (val === '') {
        update(() => {
          this.pub_options = pub
        })
        return
      }
      update(() => {
        const needle = val.toLowerCase()
        this.pub_options = pub.filter(v => v.toLowerCase().indexOf(needle) > -1)
      })
    },

    filter_cat (val, update) {
      if (val === '') {
        update(() => {
          this.cat_options = cat
        })
        return
      }
      update(() => {
        const needle = val.toLowerCase()
        this.cat_options = cat.filter(v => v.toLowerCase().indexOf(needle) > -1)
      })
    },

    filter_tag (val, update) {
      if (val === '') {
        update(() => {
          this.tag_options = tag
        })
        return
      }
      update(() => {
        const needle = val.toLowerCase()
        this.tag_options = tag.filter(v => v.toLowerCase().indexOf(needle) > -1)
      })
    }

  }
}
</script>

<style>

</style>
