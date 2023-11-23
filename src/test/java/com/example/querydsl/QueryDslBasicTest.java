package com.example.querydsl;

import com.example.querydsl.dto.MemberDto;
import com.example.querydsl.dto.UserDto;
import com.example.querydsl.entity.Member;
import com.example.querydsl.entity.QMember;
import com.example.querydsl.entity.Team;
import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.PersistenceUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static com.example.querydsl.entity.QMember.*;
import static com.example.querydsl.entity.QTeam.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
public class QueryDslBasicTest {

    @PersistenceContext
    EntityManager em;

    @PersistenceUnit
    EntityManagerFactory emf;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);

        List<Member> initialMembers = queryFactory.selectFrom(member).fetch();
        System.out.println("initialMembers = " + initialMembers.size());
        assertThat(initialMembers.size()).isEqualTo(0);

        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);

        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);

        em.flush();
        em.clear();

        List<Member> afterInitialMembers = queryFactory.selectFrom(member).fetch();
        System.out.println("afterInitialMembers = " + afterInitialMembers.size());
        assertThat(afterInitialMembers.size()).isEqualTo(4);
    }

    @Test
    public void startJPQL() {
        Member member = em
                .createQuery("SELECT m FROM Member m WHERE m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertEquals(member.getUsername(), "member1");
    }

    @Test
    public void startQueryDsl() {
        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assert findMember != null;

        assertEquals(findMember.getUsername(), "member1");
    }

    @Test
    public void search() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1").and(member.age.eq(10)))
                .fetchOne();

        assert findMember != null;
        assertEquals(findMember.getUsername(), "member1");
    }

    @Test
    public void searchNoAnd() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();

        assert findMember != null;
        assertEquals(findMember.getUsername(), "member1");
    }

    @Test
    public void resultFetch() {
        List<Member> fetch = queryFactory.selectFrom(member).fetch();

        Member fetchOne = queryFactory.selectFrom(member).fetchOne();

        Member fetchFirst = queryFactory.selectFrom(member).fetchFirst();

    }

    @Test
    public void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member1", 100));
        em.persist(new Member("member2", 100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member1 = result.get(0);
        Member member2 = result.get(1);
        Member member3 = result.get(2);

        assertEquals(member1.getUsername(), "member1");
        assertEquals(member2.getUsername(), "member2");
        assertNull(member3.getUsername());
    }

    @Test
    public void paging1() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        int totalCount = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .fetch()
                .size();

        System.out.println("totalCount = " + totalCount);

        assertEquals(result.size(), 2);
    }

    @Test
    public void aggregation() {
        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);

        assertEquals(tuple.get(member.count()), 4);
        assertEquals(tuple.get(member.age.sum()), 100);
        assertEquals(tuple.get(member.age.avg()), 25);
        assertEquals(tuple.get(member.age.max()), 40);
        assertEquals(tuple.get(member.age.min()), 10);
    }

    @Test
    public void groupBy() {
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertEquals(teamA.get(team.name), "teamA");
        assertEquals(teamA.get(member.age.avg()), 15);

        assertEquals(teamB.get(team.name), "teamB");
        assertEquals(teamB.get(member.age.avg()), 35);
    }

    /**
     * 조인
     * */
    @Test
    public void join() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인
     * */
    @Test
    public void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        // 세타조인을 하려면 이렇게 from 절에 조인하고자 하는 테이블을 여러개 넣으면 된다.
        List<Member> result =
                queryFactory
                        .select(member)
                        .from(member, team)
                        .where(member.username.eq(team.name))
                        .fetch();

        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    @Test
    public void join_on_filtering() {
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team) // 이렇게 넣으면, 흔히 알고있는 member.team_id = team.id 가 걸리는 것
                .on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * ON JOIN
     * */
    @Test
    public void on_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team) // 이렇게 하나만 넣으면 id랑 상관없이 on 조건으로 조인을 하여 가져온다 (left join 이니까 Team이 없거나 조건에 해당하지 않아 null 값인것도 가져옴)
                .on(member.username.eq(team.name))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void fetchJoinNo() {
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assert findMember != null;

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());

        assertFalse(loaded);
    }

    @Test
    public void fetchJoinYes() {
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        assert findMember != null;

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());

        assertTrue(loaded);
    }

    @Test
    public void subQuery() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                                JPAExpressions
                                        .select(memberSub.age.max())
                                        .from(memberSub)
                        )
                ).fetch();

        assertThat(result).extracting("age").containsExactly(40);
    }

    @Test
    public void subQueryGoe() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                                JPAExpressions
                                        .select(memberSub.age.avg())
                                        .from(memberSub)
                        )
                ).fetch();

        assertThat(result).extracting("age").containsExactly(30, 40);
    }

    @Test
    public void subQueryIn() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                                JPAExpressions
                                        .select(memberSub.age)
                                        .from(memberSub)
                                        .where(memberSub.age.gt(10))
                        )
                ).fetch();

        assertThat(result).extracting("age").containsExactly(20, 30, 40);
    }

    /**
     * 참고로 서브쿼리는 이 JPQL이 FROM절에서는 지원하지 않기 때문에 Querydsl에서도 못쓴다.
     * */
    @Test
    public void selectSubQuery() {
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();


        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void basicCase() {
        List<String> result = queryFactory
                .select(member.age.when(10).then("열살").when(20).then("스무살").otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }

        List<String> result2 = queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result2) {
            System.out.println("s = " + s);
        }
    }

    @Test
    public void concat() {
        List<String> fetch = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        for (String s : fetch) {
            System.out.println("s = " + s);
        }
    }

    @Test
    public void simpleProjection() {
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    /**
     * Tuple 이라는 건 여러 데이터를 하나로 담아서 뽑아 사용할 수 있게 해주는 객체인데
     * 이 녀석의 패키지가 querydsl이다. 즉, 리포지토리 레벨에서 튜플을 사용하는 건 괜찮아도 이것을 서비스 또는 더 나아가 컨트롤러에서까지
     * 접근하게 하면 좋은 설계라고 할 수 없다. 그렇기 때문에 리포지토리 레벨에서만 가급적 사용하고 서비스나 컨트롤러에는 새로운 DTO로 반환해주어야 한다.
     * */
    @Test
    public void tupleProjection() {
        List<Tuple> result = queryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);

            System.out.println("username = " + username);
            System.out.println("age = " + age);
        }
    }

    @Test
    public void findDtoByJPQL() {
        List<MemberDto> result = em
                .createQuery(
                        "SELECT new com.example.querydsl.dto.MemberDto(m.username, m.age) " +
                        "FROM Member m",
                        MemberDto.class)
                .getResultList();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    public void findDtoByQueryDslSetter() {
        // bean 방식은 객체를 만들고 그 객체의 setter로 데이터를 넣어주는 방식
        // setter를 사용하기 때문에 필드명이 일치해야 함
        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    public void findDtoByQueryDslField() {
        // fields 방식은 객체를 만들고 그 객체의 field에 직접 데이터를 넣어주는 방식
        // field에 직접 접근하기 때문에 필드명이 일치해야 함
        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    public void findDtoByQueryDslConstructor() {
        // constructor 방식은 생성자를 사용해서 객체를 만들고 돌려주는 방식
        // 그래서 생성자에 들어가는 타입과 파라미터 개수가 중요.
        List<UserDto> result = queryFactory
                .select(Projections.constructor(UserDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (UserDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    public void findDtoByQueryDslFieldAs() {
        // fields 방식은 객체를 만들고 그 객체의 field에 직접 데이터를 넣어주는 방식
        // field에 직접 접근하기 때문에 필드명이 일치해야 하므로 username을 name으로 치환해야함
        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"),
                        member.age))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }
    }

    @Test
    public void findDtoByQueryDslFieldAndSubQuery() {
        QMember memberSub = new QMember("memberSub");

        // Select절에 프로젝션으로 SubQuery를 사용할 때, DTO로 변환하는 과정에서 DTO가 가지고 있는 필드에 별칭에 넣어주는 방법은
        // 다음처럼 ExpressionUtils를 사용한다.

        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"),
                        ExpressionUtils.as(JPAExpressions.
                                select(memberSub.age.max())
                                .from(memberSub),
                        "age")))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }
    }

    @Test
    public void booleanBuilder() {
        String username = "member1";
        int age = 10;

        List<Member> result = searchMember1(username, age);

        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {

        BooleanBuilder builder = new BooleanBuilder();

        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return queryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }

    @Test
    public void dynamicQuery_WhereParam() {
        String username = "member1";
        int age = 10;

        List<Member> result = searchMember2(username, age);

        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {

        return queryFactory
                .selectFrom(member)
                .where(usernameEq(usernameCond), ageEq(ageCond))
                .fetch();
    }

    private BooleanExpression ageEq(Integer ageCond) {

        if (ageCond == null) {
            return null;
        }

        return member.age.eq(ageCond);
    }

    private BooleanExpression usernameEq(String usernameCond) {

        if (usernameCond == null) {
            return null;
        }

        return member.username.eq(usernameCond);
    }

    private BooleanExpression allEq(String usernameCond, Integer ageCond) {
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    @Test
    public void bulkUpdate() {
        long count = queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        // 벌크 연산은 영속성 컨텍스트와 무관하게 바로 DB에 쿼리를 날리기 때문에 벌크 연산 후 같은 트랜잭션 내에서 같은 데이터를 가져오는 경우
        // 벌크 연산이 적용되지 않은 영속성 컨텍스트에 남아있는 1차 캐시의 데이터를 가져오기 때문에 반드시 영속성 컨텍스트를 초기화 해주어야 한다.
        em.flush();
        em.clear();

        List<Member> afterUpdateMembers = queryFactory.selectFrom(member).fetch();
        for (Member afterUpdateMember : afterUpdateMembers) {
            System.out.println("afterUpdateMember = " + afterUpdateMember);
        }
    }

    @Test
    public void bulkAdd() {
        long count = queryFactory
                .update(member)
                .set(member.age, member.age.add(1))
                .execute();

        em.flush();
        em.clear();

        List<Member> members = queryFactory.selectFrom(member).fetch();
        for (Member member1 : members) {
            System.out.println("member1 = " + member1);
        }
    }

    @Test
    public void bulkMultiply() {
        long count = queryFactory
                .update(member)
                .set(member.age, member.age.multiply(2))
                .execute();

        em.flush();
        em.clear();

        List<Member> members = queryFactory.selectFrom(member).fetch();
        for (Member member1 : members) {
            System.out.println("member1 = " + member1);
        }
    }

    @Test
    public void bulkDelete() {
        long count = queryFactory.delete(member).where(member.age.lt(18)).execute();

        em.flush();
        em.clear();

        List<Member> members = queryFactory.selectFrom(member).fetch();
        for (Member member1 : members) {
            System.out.println("member1 = " + member1);
        }
    }

    @Test
    public void sqlFunction() {
        List<String> result = queryFactory
                .select(Expressions.stringTemplate(
                        "function('replace', {0}, {1}, {2})",
                        member.username,
                        "member",
                        "M"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }
}