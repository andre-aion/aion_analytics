from datetime import datetime, timedelta

from bokeh.models import HTMLTemplateFormatter

DATEFORMAT = '%Y-%m-%d %H:%M:%S'
config = {

    'dates': {
        'DATEFORMAT': '%Y-%m-%d %H:%M:%S',
        'last_date': datetime.today() - timedelta(days=1),
        'current_year_start':datetime(datetime.today().year,1,1,0,0,0),
        'DAYS_TO_LOAD':30
    },
    'colors': ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231',
               '#911eb4', '#46f0f0', '#f032e6', '#bcf60c', '#fabebe',
               '#008080', '#e6beff', '#9a6324', '#fffac8', '#800000',
               '#aaffc3', '#808000', '#ffd8b1', '#000075', '#808080',
               '#ffffff', '#000000'],
    'formatters':
        {
        'Unlikely': HTMLTemplateFormatter(template="""
                <div style="background-color:<%= 
                    (function colorfromint(){
                        var thresh_1 = 8;
                        var thresh_2 = 15;
                        if (typeof Unlikely == 'string')
                            {return("black")}
                        else{
                            if(Unlikely < thresh_1 )
                                {return("green")}
                            else if (Unlikely < thresh_2)
                                {return("#CCCC00")}
                            else if (Unlikely >= thresh_2)
                                {return("red")}
                        }

                    }()) %>;
                    color: white">
                <%= value %>
                </div>
            """),
            'Seldom': HTMLTemplateFormatter(template="""
                <div style="background-color:<%= 
                    (function colorfromint(){
                        var thresh_1 = 8;
                        var thresh_2 = 15;
                        if (typeof Seldom == 'string')
                            {return("black")}
                        else{
                            if(Seldom < thresh_1 )
                                {return("green")}
                            else if (Seldom < thresh_2)
                                {return("#CCCC00")}
                            else if (Seldom >= thresh_2)
                                {return("red")}
                        }

                    }()) %>;
                    color: white">
                <%= value %>
                </div>
            """),

            'Occaisional': HTMLTemplateFormatter(template="""
                <div style="background-color:<%= 
                    (function colorfromint(){
                        var thresh_1 = 8;
                        var thresh_2 = 15;
                        if (typeof Occaisional == 'string')
                            {return("black")}
                        else{
                            if(Occaisional < thresh_1 )
                                {return("green")}
                            else if (Occaisional < thresh_2)
                                {return("#CCCC00")}
                            else if (Occaisional >= thresh_2)
                                {return("red")}
                        }

                    }()) %>;
                    color: white">
                <%= value %>
                </div>
            """),
            'Likely': HTMLTemplateFormatter(template="""
                <div style="background-color:<%= 
                    (function colorfromint(){
                        var thresh_1 = 8;
                        var thresh_2 = 15;
                        if (typeof Likely == 'string')
                            {return("black")}
                        else{
                            if(Likely < thresh_1 )
                                {return("green")}
                            else if (Likely < thresh_2)
                                {return("#CCCC00")}
                            else if (Likely >= thresh_2)
                                {return("red")}
                        }

                    }()) %>;
                    color: white">
                <%= value %>
                </div>
            """),
            'Definite': HTMLTemplateFormatter(template="""
                <div style="background-color:<%= 
                    (function colorfromint(){
                        var thresh_1 = 8;
                        var thresh_2 = 15;
                        if (typeof Definite == 'string')
                            {return("black")}
                        else{
                            if(Definite < thresh_1 )
                                {return("green")}
                            else if (Definite < thresh_2)
                                {return("#CCCC00")}
                            else if (Definite >= thresh_2)
                                {return("red")}
                        }

                    }()) %>;
                    color: white">
                <%= value %>
                </div>
            """),



        }
}