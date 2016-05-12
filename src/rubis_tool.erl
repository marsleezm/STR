-module(rubis_tool).

-include("rubis.hrl").

-export([random_num/2, get_key/2, now_nsec/0, non_uniform_random/4, 
        create_item/10, create_bid/6, create_user/9, 
        load_transition/0, get_next_state/3, create_comment/6, create_buy_now/4, 
		translate_op/1]).

get_next_state(PreviousStates, Dict, CurrentState) ->
    {_, T} = dict:fetch(CurrentState, Dict),
    Num = random:uniform(),
    %lager:info("T is ~p", [T]),
    S = find_next_state(T, Num, 0.0, 0, CurrentState),
    End = length(T),
    Previous = End -1,
    case S of
        Previous -> %% Go back to previous state
            case PreviousStates of [] ->  {[], 1};
                                  [P1|P2] -> {P2, P1}
            end;
        End -> %% Terminate session
            {[], 1}; 
        CurrentState ->
            {PreviousStates, CurrentState};
        _ ->
            case dict:fetch(S, Dict) of
                {back, _} -> {[CurrentState|PreviousStates], S};
                {not_back, _} -> {[], S} 
            end
    end.

find_next_state([], _Num, _ProbAcc, _Acc, Current) ->
    Current;
find_next_state([H|T], Num, ProbAcc, Acc, Current) ->
    case ProbAcc > Num of
        true -> Acc;
        false -> find_next_state(T, Num, ProbAcc+H, Acc+1, Current)
    end.

load_transition() ->
    FileName = basho_bench_config:get(transition_file),
    NumCols = 27,
    NumRows = 29,
    {ok, Device} = file:open(FileName, [read]),
    io:get_line(Device, ""),
    io:get_line(Device, ""),
    io:get_line(Device, ""),
    L = droplast(io:get_line(Device, "")),
    [_|Headers0] = re:split(L, "\\t", [{return, list}]),
    %% Remove transaction waiting time
    Headers = droplast(Headers0),
    Dict = get_all_lines(Device, dict:new(), NumCols, NumRows, Headers, 0),
    %lager:info("Info ~p", [Dict]),
    file:close(Device),
    dict:fold(fun(K, V, D) ->
            %lager:info("K is ~w, V is ~w", [K, V]),
            case lists:nth(NumRows-1, V) of
                0 ->
                    dict:store(K, {not_back, V}, D);
                _ -> dict:store(K, {back, V}, D)
            end end, dict:new(), Dict).

get_all_lines(_, Dict, _NumCols, NumRows, _Headers, NumRows) ->
    Dict;
get_all_lines(Device, Dict, NumCols, NumRows, Headers, LineNum) ->
    case io:get_line(Device, "") of
        eof -> Dict;
        Line0 -> 
            Line = droplast(Line0),
            [_CurrentGo|Splitted0] = re:split(Line, "\\t", [{return, list}]),            
            Splitted = droplast(Splitted0),
            %io:format("Splitted ~p", [Splitted0]),
            {_, ND} = lists:foldl(fun(V, {Acc, D}) -> 
                    %Header = lists:nth(Acc, Headers),
                    case V of
                        "0" -> {Acc+1, dict:append(Acc, 0, D)};
                        _ -> {Acc+1, dict:append(Acc, list_to_float(V), D)}
                    end end, {1, Dict}, Splitted),
            get_all_lines(Device, ND, NumCols, NumRows, Headers, LineNum+1)
    end.

non_uniform_random(Type, X, Min, Max) ->
    A = random_num(0, X),
    B = random_num(Min, Max),
    Bor = A bor B,
    (( Bor + Type) rem (Max - Min + 1)) + Min.

random_num(Start, End) ->
    random:uniform(End-Start+1) + Start - 1.

create_item(Name, Description, InitialPrice, Quantity, ReservePrice, BuyNow, StartDate, EndDate, UserId, CategoryId) ->
    #item{i_name=Name, i_description = Description, i_quantity = Quantity, i_init_price=InitialPrice, 
            i_reserve_price = ReservePrice, i_buy_now = BuyNow, i_nb_of_bids = 0, i_max_bid = 0, i_start_date = StartDate, 
            i_end_date = EndDate, i_seller = UserId, i_category = CategoryId}. 

create_bid(UserId, ItemId, Qty, Bid, MaxBid, Now) ->
    #bid{b_user_id=UserId, b_item_id=ItemId, b_qty=Qty, b_bid=Bid, b_max_bid=MaxBid, b_date=Now}.

create_buy_now(BuyerId, ItemId, Qty, Now) ->
    #buy_now{bn_buyer_id=BuyerId, bn_item_id=ItemId, bn_qty=Qty, bn_date=Now}.

create_user(FirstName, LastName, NickName, Password, Email, Now, Rating, Balance, RegionId) ->
    #user{u_firstname=FirstName, u_lastname=LastName, u_nickname=NickName, u_password=Password, u_comment_nodes=[],
            u_email=Email, u_rating=Rating, u_creation_date=Now, u_balance=Balance, u_region=RegionId, u_num_comments=0, 
            u_bids= [], u_sellings = [], u_bought=[]}.

create_comment(FromId, ToId, ItemId, Rating, Now, Comment) ->
    #comment{c_item_id=ItemId, c_from_id=FromId, c_to_id=ToId, c_rating=Rating, c_date=Now, c_comment=Comment}.

get_key({{P0, P1}, P2}, Type) ->
    integer_to_list(P0)++"_"++integer_to_list(P1)++"_"++integer_to_list(P2)++type_to_str(Type);
get_key({P1, P2}, Type) ->
    integer_to_list(P1)++"_"++integer_to_list(P2)++type_to_str(Type);
get_key(P, Type) ->
    integer_to_list(P)++type_to_str(Type).

type_to_str(Type) ->
    case Type of
        item -> "_item";
        region -> "_region";
        category -> "_category";
        comment -> "_comment";
        user -> "_user";
        bid -> "_bid";
        buy_now -> "_buynow";
        regionnewitems -> "_regionnewitems";
        categorynewitems -> "_categorynewitems";
        lastbuynow -> "_lastbuynow";
        lastbid -> "_lastbid";
        lastitem -> "_lastitem"
    end.

%%%%%% private funs %%%%%%%%%%%

now_nsec() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

droplast(L)->
    droplast(L, []).

droplast([_], Acc) ->
    lists:reverse(Acc);
droplast([X|H], Acc) ->
    droplast(H, [X|Acc]).

translate_op(Op) ->
    case Op of
        1 ->    home;
        2 -> register;
        3 -> register_user;
        4 -> browse;
        5 -> browse_categories;
        6 -> search_items_in_category;
        7 -> browse_regions;
        8 -> browse_categories_in_region;
        9 -> search_items_in_region;
        10 -> view_item;
        11 -> view_user_info;
        12 -> view_bid_history;
        13 -> buy_now_auth;
        14 -> buy_now;
        15 -> store_buy_now;
        16 -> put_bid_auth;
        17 -> put_bid;
        18 -> store_bid;
        19 -> put_comment_auth;
        20 -> put_comment;
        21 -> store_comment;
        22 -> sell;
        23 -> select_category_to_sell_item;
        24 -> sell_item_form;
        25 -> register_item;
        26 -> about_me_auth;
        27 -> about_me;
        _ -> Op
    end.

